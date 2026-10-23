"""Invocation handles: the only thing a shared server can present (ADR 0239).

"Bundle, identity and request contract" 3 and 6. The proxy verifies the caller
token, then forwards a fresh 256-bit random handle instead of anything the
caller sent. The handle names one invocation context (installation,
application, agent, alias, policy generation, the binding the proxy admitted,
the stamp of the agent's own membership row, optional run and work item) for
as long as that invocation lives and the signed caller expiry allows, and it is
removed on completion or cancellation. The binding pins the account: admission
refuses ``binding_mismatch`` when the agent's current binding is another one, so
an account replacement cannot redirect an in-flight call. The membership stamp
lets the handle survive a policy generation change another member caused
(``admission``). A session ID is never identity: lookups are by handle only,
so a session ID presented as a handle is unknown.

The registry stores ``sha256(handle)`` rather than the handle, so a heap dump
of the registry alone does not hand out live handles, and neither ``repr`` nor
``str`` of an ``Invocation`` carries one.

Bounds are per application: 1024 active invocations and 4096 sessions.
Overflow refuses before any randomness is drawn and before anything is stored
("overflow refuses without allocation"); expired invocations are pruned first
so the bound counts live ones. One ``threading.Lock`` guards every map: the
proxy registers on its event loop while tests drive the registry from another
thread.
"""

from __future__ import annotations

import base64
import hashlib
import re
import secrets
import threading
import time
from collections.abc import Callable
from dataclasses import dataclass

from ..caller import nonblank_installation_ok
from .wire import CodedError

MAX_INVOCATIONS = 1024
MAX_SESSIONS = 4096
HANDLE_PREFIX = "crh1."

REGISTRY_INVOCATIONS_FULL = "invocations_full"
REGISTRY_SESSIONS_FULL = "sessions_full"
REGISTRY_FOREIGN = "foreign_principal"
REGISTRY_INVALID = "invalid_context"
REGISTRY_SESSION_UNKNOWN = "session_unknown"
REGISTRY_SESSION_MISMATCH = "session_principal_mismatch"
HANDLE_UNKNOWN = "handle_unknown"
HANDLE_EXPIRED = "handle_expired"

_HANDLE_BYTES = 32
# Far above a real handle (48 characters); bounds the work one lookup does.
_MAX_HANDLE_CHARS = 128
_MAX_SESSION_ID_CHARS = 256
_UUID = re.compile(r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$")
_ALIAS = re.compile(r"^[a-z0-9]([a-z0-9-]{0,61}[a-z0-9])?$")
_STAMP = re.compile(r"^[0-9a-f]{64}$")


class RegistryRefusal(CodedError):
    """A registry refusal. ``str(exc)`` is the code; it never carries a handle."""


@dataclass(frozen=True)
class Principal:
    """Who an invocation acts for. ``agent`` is a display name and never authorizes."""

    installation: str
    application: str
    agent_id: str
    agent: str
    policy_generation: int


@dataclass(frozen=True)
class InvocationContext:
    """What one handle authorizes the resolver to look up, and until when."""

    principal: Principal
    alias: str
    # The signed caller exp in epoch seconds; invalid once clock() >= expires_at.
    expires_at: int
    # The binding ID the proxy's per-call read admitted (canonical UUID).
    binding: str
    # ``admission.membership_stamp`` of the agent's alias row as the proxy read
    # it (64 lowercase hex).
    membership: str
    run: str | None = None
    work_item: str | None = None
    session_id: str | None = None


def _is_int(value: object) -> bool:
    return isinstance(value, int) and not isinstance(value, bool)


def installation_ok(value: object) -> bool:
    """A resolver installation is always nonblank: ADR 0239's rule, defined in ``caller``."""

    return nonblank_installation_ok(value)


def uuid_ok(value: object) -> bool:
    return isinstance(value, str) and _UUID.fullmatch(value) is not None


def _principal_ok(principal: object) -> bool:
    if not isinstance(principal, Principal):
        return False
    return (
        installation_ok(principal.installation)
        and uuid_ok(principal.application)
        and uuid_ok(principal.agent_id)
        and isinstance(principal.agent, str)
        and bool(principal.agent)
        and _is_int(principal.policy_generation)
        and principal.policy_generation >= 1
    )


def _session_id_ok(value: object) -> bool:
    return (
        isinstance(value, str)
        and 0 < len(value) <= _MAX_SESSION_ID_CHARS
        and value.isascii()
        and value.isprintable()
    )


def _digest(handle: str) -> bytes | None:
    """The stored key for ``handle``, or None when it cannot be a handle at all."""

    if not isinstance(handle, str) or len(handle) > _MAX_HANDLE_CHARS or not handle.isascii():
        return None
    return hashlib.sha256(handle.encode("ascii")).digest()


class Invocation:
    """One registered invocation. Its ``repr`` and ``str`` never include the handle."""

    __slots__ = ("_handle", "_registry", "context")

    def __init__(self, handle: str, context: InvocationContext, registry: HandleRegistry) -> None:
        self._handle = handle
        self._registry = registry
        self.context = context

    @property
    def handle(self) -> str:
        return self._handle

    def close(self) -> None:
        """Remove this invocation. Idempotent."""

        self._registry.remove(self._handle)

    def __repr__(self) -> str:
        return f"Invocation(context={self.context!r})"

    __str__ = __repr__


class HandleRegistry:
    """The live invocations and sessions of one installation's one application."""

    def __init__(
        self,
        *,
        installation: str,
        application: str,
        max_invocations: int = MAX_INVOCATIONS,
        max_sessions: int = MAX_SESSIONS,
        clock: Callable[[], float] = time.time,
    ) -> None:
        if not installation_ok(installation):
            raise ValueError("the registry installation is not a valid installation ID")
        if not uuid_ok(application):
            raise ValueError("the registry application is not a canonical UUID")
        if not _is_int(max_invocations) or max_invocations < 1:
            raise ValueError("max_invocations must be a positive integer")
        if not _is_int(max_sessions) or max_sessions < 1:
            raise ValueError("max_sessions must be a positive integer")
        self._installation = installation
        self._application = application
        self._max_invocations = max_invocations
        self._max_sessions = max_sessions
        self._clock = clock
        self._lock = threading.Lock()
        self._invocations: dict[bytes, InvocationContext] = {}
        self._sessions: dict[str, Principal] = {}

    @property
    def installation(self) -> str:
        return self._installation

    @property
    def application(self) -> str:
        return self._application

    @property
    def max_invocations(self) -> int:
        return self._max_invocations

    @property
    def max_sessions(self) -> int:
        return self._max_sessions

    @property
    def active_invocations(self) -> int:
        with self._lock:
            self._prune()
            return len(self._invocations)

    @property
    def sessions(self) -> int:
        with self._lock:
            return len(self._sessions)

    def _foreign(self, principal: Principal) -> bool:
        return (
            principal.installation != self._installation
            or principal.application != self._application
        )

    def _prune(self) -> None:
        now = self._clock()
        lapsed = [key for key, context in self._invocations.items() if now >= context.expires_at]
        for key in lapsed:
            del self._invocations[key]

    def open_session(self, session_id: str, principal: Principal) -> None:
        """Open ``session_id`` for ``principal``; idempotent for an equal principal."""

        if isinstance(principal, Principal) and self._foreign(principal):
            raise RegistryRefusal(REGISTRY_FOREIGN)
        if not _principal_ok(principal) or not _session_id_ok(session_id):
            raise RegistryRefusal(REGISTRY_INVALID)
        with self._lock:
            existing = self._sessions.get(session_id)
            if existing is not None:
                if existing != principal:
                    raise RegistryRefusal(REGISTRY_SESSION_MISMATCH)
                return
            if len(self._sessions) >= self._max_sessions:
                raise RegistryRefusal(REGISTRY_SESSIONS_FULL)
            self._sessions[session_id] = principal

    def close_session(self, session_id: str) -> None:
        """Close ``session_id`` and revoke every invocation bound to it."""

        with self._lock:
            self._sessions.pop(session_id, None)
            bound = [
                key
                for key, context in self._invocations.items()
                if context.session_id == session_id
            ]
            for key in bound:
                del self._invocations[key]

    def _valid(self, context: InvocationContext) -> bool:
        if not isinstance(context, InvocationContext) or not _principal_ok(context.principal):
            return False
        if not isinstance(context.alias, str) or _ALIAS.fullmatch(context.alias) is None:
            return False
        if not _is_int(context.expires_at):
            return False
        if not uuid_ok(context.binding):
            return False
        if not isinstance(context.membership, str) or _STAMP.fullmatch(context.membership) is None:
            return False
        if (context.run is None) != (context.work_item is None):
            return False
        if context.run is not None and not (uuid_ok(context.run) and uuid_ok(context.work_item)):
            return False
        return context.session_id is None or _session_id_ok(context.session_id)

    def register(self, context: InvocationContext) -> Invocation:
        """Register ``context`` and answer its invocation with a fresh handle."""

        if isinstance(context, InvocationContext) and isinstance(context.principal, Principal):
            if self._foreign(context.principal):
                raise RegistryRefusal(REGISTRY_FOREIGN)
        if not self._valid(context):
            raise RegistryRefusal(REGISTRY_INVALID)
        with self._lock:
            if self._clock() >= context.expires_at:
                raise RegistryRefusal(REGISTRY_INVALID)
            if context.session_id is not None:
                session = self._sessions.get(context.session_id)
                if session is None:
                    raise RegistryRefusal(REGISTRY_SESSION_UNKNOWN)
                if session != context.principal:
                    raise RegistryRefusal(REGISTRY_SESSION_MISMATCH)
            self._prune()
            # The bound refuses before any randomness is drawn or anything stored.
            if len(self._invocations) >= self._max_invocations:
                raise RegistryRefusal(REGISTRY_INVOCATIONS_FULL)
            while True:
                # Through the module attribute, so one patch point covers every draw.
                raw = secrets.token_bytes(_HANDLE_BYTES)
                handle = HANDLE_PREFIX + base64.urlsafe_b64encode(raw).rstrip(b"=").decode("ascii")
                key = hashlib.sha256(handle.encode("ascii")).digest()
                if key not in self._invocations:
                    break
            self._invocations[key] = context
        return Invocation(handle, context, self)

    def remove(self, handle: str) -> bool:
        """Revoke ``handle``. True only when it was live in this registry."""

        key = _digest(handle)
        if key is None:
            return False
        with self._lock:
            return self._invocations.pop(key, None) is not None

    def admit(self, handle: str) -> InvocationContext:
        """The context ``handle`` names, or ``handle_unknown`` / ``handle_expired``.

        An expired handle is removed as it is refused, so it answers
        ``handle_expired`` once and ``handle_unknown`` after that.
        """

        key = _digest(handle)
        if key is None:
            raise RegistryRefusal(HANDLE_UNKNOWN)
        with self._lock:
            context = self._invocations.get(key)
            if context is None:
                raise RegistryRefusal(HANDLE_UNKNOWN)
            if self._clock() >= context.expires_at:
                del self._invocations[key]
                raise RegistryRefusal(HANDLE_EXPIRED)
            return context
