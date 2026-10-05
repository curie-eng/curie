"""The per-turn channel read capability holder (ADR 0100, #2877).

The kernel sends a ``ChannelReadCapability`` on ``/v1/event`` and on each
``/v1/steer``. The session hands it to this holder at turn open (``begin``), at
every steer (``steer``) and on every terminal path (``end``). The read tools
read the current credential through ``current`` and never see anything else.

The admitted scope (agent, deployment, logical turn, default channel) and the
highest accepted generation are kept apart from the nullable credential until
``end``. So a null steer clears only the credential, and a later steer can
still be checked against the scope the turn was opened with. The runner cannot
verify the API's signature; it reads scope and generation from the token's
middle segment purely to refuse a capability that could not belong to this
turn. The platform route is the authority on every read.

Nothing here renders the token or the URL: not ``repr``, not ``str``, not a log
line. A capability whose URL is not the trusted platform origin, or whose path
is not the read route, is refused and cleared, so a token is only ever
presented to the platform that minted it. The canvas tools (ADR 0200) derive
their route from that admitted read route (``canvas_url``); no second URL is
ever accepted.
"""

from __future__ import annotations

import base64
import binascii
import json
import logging
from dataclasses import dataclass
from typing import Any
from urllib.parse import urlsplit, urlunsplit

from aci_protocol import ChannelReadCapability, Event

logger = logging.getLogger(__name__)

TOKEN_PREFIX = "chr"
READ_PATH_SUFFIX = "/channel-read"
CANVAS_PATH_SUFFIX = "/channel-canvas"
_MAX_TOKEN_LENGTH = 4096
_DEFAULT_PORTS = {"http": 80, "https": 443}

Origin = tuple[str, str, int]


@dataclass(frozen=True)
class _Scope:
    agent: str
    deployment: str
    turn: str
    default: tuple[str, str] | None


@dataclass(frozen=True)
class _Claims:
    scope: _Scope
    gen: int


def url_origin(url: str | None) -> Origin | None:
    """``(scheme, host, port)`` of an HTTP or HTTPS URL, or None."""

    if not url:
        return None
    try:
        parts = urlsplit(url.strip())
        port = parts.port
    except ValueError:
        return None
    scheme = parts.scheme.lower()
    host = (parts.hostname or "").lower()
    if scheme not in _DEFAULT_PORTS or not host:
        return None
    return (scheme, host, port if port is not None else _DEFAULT_PORTS[scheme])


def canvas_url(capability: ChannelReadCapability) -> str:
    """The sibling canvas route on the capability's own origin (ADR 0200).

    Derived from a capability the holder already admitted, so its origin and
    its ``/channel-read`` path were checked; no second URL is ever accepted.
    """

    parts = urlsplit(capability.url.strip())
    if not parts.path.endswith(READ_PATH_SUFFIX):
        raise ValueError("not an admitted channel read route")
    path = parts.path[: -len(READ_PATH_SUFFIX)] + CANVAS_PATH_SUFFIX
    return urlunsplit((parts.scheme, parts.netloc, path, "", ""))


def _decode_claims(token: str) -> _Claims | None:
    """Scope and generation from ``chr.<claims>.<sig>``, unverified, or None."""

    if len(token) > _MAX_TOKEN_LENGTH:
        return None
    parts = token.split(".")
    if len(parts) != 3 or parts[0] != TOKEN_PREFIX or not parts[1] or not parts[2]:
        return None
    segment = parts[1]
    try:
        raw = base64.urlsafe_b64decode(segment + "=" * (-len(segment) % 4))
        data: Any = json.loads(raw)
    except (binascii.Error, ValueError):
        return None
    if not isinstance(data, dict):
        return None
    agent, deployment, turn = data.get("agent"), data.get("deployment"), data.get("turn")
    gen, default = data.get("gen"), data.get("default")
    if not all(isinstance(value, str) and value for value in (agent, deployment, turn)):
        return None
    if isinstance(gen, bool) or not isinstance(gen, int) or gen <= 0:
        return None
    pair: tuple[str, str] | None
    if default is None:
        pair = None
    elif (
        isinstance(default, dict)
        and isinstance(default.get("kind"), str)
        and isinstance(default.get("address"), str)
    ):
        pair = (default["kind"], default["address"])
    else:
        return None
    return _Claims(_Scope(str(agent), str(deployment), str(turn), pair), gen)


class ChannelReadTurn:
    """The channel read credential of the one live turn, or none."""

    def __init__(self, *, trusted_origin: Origin | None) -> None:
        self._trusted_origin = trusted_origin
        self._scope: _Scope | None = None
        self._highest = 0
        # One attribute, replaced whole under no await: a read sees the old
        # credential or the new one, never a mix of the two.
        self._credential: ChannelReadCapability | None = None
        self._replay_tainted = False

    def __repr__(self) -> str:
        return (
            f"ChannelReadTurn(admitted={self._scope is not None}, "
            f"credential={self._credential is not None})"
        )

    __str__ = __repr__

    def _trusted(self, capability: ChannelReadCapability) -> bool:
        if self._trusted_origin is None:
            return False
        if url_origin(capability.url) != self._trusted_origin:
            return False
        parts = urlsplit(capability.url.strip())
        return parts.path.endswith(READ_PATH_SUFFIX) and not parts.query and not parts.fragment

    def _admissible(self, capability: ChannelReadCapability) -> _Claims | None:
        if not self._trusted(capability):
            logger.warning("channel read capability refused: untrusted read route")
            return None
        claims = _decode_claims(capability.token)
        if claims is None:
            logger.warning("channel read capability refused: unreadable scope")
        return claims

    def begin(self, event: Event) -> None:
        """A new turn: scope, generation and credential all come from ``event``."""

        capability = event.channel_read
        claims = self._admissible(capability) if capability is not None else None
        if capability is None or claims is None:
            self._scope, self._highest, self._credential = None, 0, None
            return
        self._scope, self._highest, self._credential = claims.scope, claims.gen, capability

    def steer(self, event: Event | None) -> None:
        """Apply a steer's capability under the admitted scope and generation.

        Null or omitted clears the credential only. A capability with no
        admitted scope, another scope, an untrusted route or an unreadable
        token is rejected and clears the credential. One at or below the
        highest accepted generation is stale and changes nothing.
        """

        capability = event.channel_read if event is not None else None
        if capability is None:
            self._credential = None
            return
        if self._scope is None:
            logger.warning("channel read steer rejected: no admitted scope")
            self._credential = None
            return
        claims = self._admissible(capability)
        if claims is None:
            self._credential = None
            return
        if claims.scope != self._scope:
            logger.warning("channel read steer rejected: scope differs from the admitted turn")
            self._credential = None
            return
        if claims.gen <= self._highest:
            logger.warning("channel read steer rejected: stale generation")
            return
        self._highest, self._credential = claims.gen, capability

    def end(self) -> None:
        """Terminal: forget the scope, the generation and the credential."""

        self._scope, self._highest, self._credential = None, 0, None

    def current(self) -> ChannelReadCapability | None:
        return self._credential

    @property
    def replay_tainted(self) -> bool:
        """Whether this SDK session has received a channel read body.

        Survives ``end``: the body stays in the SDK session's own context, so
        native replay stays off until a new SDK session replaces it.
        """

        return self._replay_tainted

    def mark_replay_tainted(self) -> None:
        self._replay_tainted = True

    def clear_replay_taint(self) -> None:
        self._replay_tainted = False
