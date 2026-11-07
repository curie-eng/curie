"""Authenticated delivery of the existing channel reply wire."""

from collections.abc import Hashable, Mapping
from dataclasses import dataclass
from types import MappingProxyType
from typing import Protocol

from channel_protocol import ReplyAck, ReplyEvent


class ReplyEventTransport[Route](Protocol):
    """Deliver four event kinds, including wire 1.1 progress and 1.2 verbatim.

    Route is the caller's transport metadata, currently worker TargetRoute;
    endpoints and credential selectors never enter ReplyEvent. HTTP delivery
    uses the operator-selected adapter's X-Curie-Adapter-Secret, refuses missing
    credentials and redirects, and returns ReplyAck containing the opaque ref.
    Authentication is a requirement of implementations, not supplied by this
    protocol. Best effort applies only to an unreachable endpoint, never a
    responding endpoint's refusal. Delivery IDs survive retries unchanged.
    """

    async def emit(
        self,
        event: ReplyEvent,
        *,
        route: Route,
        best_effort_unreachable: bool = False,
    ) -> ReplyAck: ...


@dataclass(frozen=True)
class ReplyCapabilities:
    """Provider declarations for one kind and route, available before a model runs.

    The gateway supplies these from its configured provider identities. A missing
    token must remain an undeliverable reason even when HTTP egress is reachable.
    """

    edits_in_place: bool
    undeliverable_reason: str | None


class AuthenticatedReplyTransport[Route](ReplyEventTransport[Route], Protocol):
    """Complete kernel-facing delivery contract, including synchronous preflight."""

    def edits_in_place(self, kind: str, route: Route) -> bool: ...

    def undeliverable_reason(self, kind: str, route: Route) -> str | None: ...


class DeclaredReplyTransport[Route: Hashable]:
    """Attach explicit per-kind, per-route declarations to an HTTP transport.

    Composition supplies a complete snapshot from the gateway's provider
    configuration and replaces it when that configuration changes. Declarations
    are not inferred from HTTP reachability or put on the reply event wire.
    Unknown routes raise KeyError rather than silently becoming buffered or
    deliverable. No production composition uses this adapter until extraction.
    """

    def __init__(
        self,
        transport: ReplyEventTransport[Route],
        declarations: Mapping[tuple[str, Route], ReplyCapabilities],
    ) -> None:
        self._transport = transport
        self._declarations = MappingProxyType(dict(declarations))

    def edits_in_place(self, kind: str, route: Route) -> bool:
        return self._declarations[kind, route].edits_in_place

    def undeliverable_reason(self, kind: str, route: Route) -> str | None:
        return self._declarations[kind, route].undeliverable_reason

    async def emit(
        self,
        event: ReplyEvent,
        *,
        route: Route,
        best_effort_unreachable: bool = False,
    ) -> ReplyAck:
        _ = self._declarations[event.target.kind, route]
        return await self._transport.emit(
            event, route=route, best_effort_unreachable=best_effort_unreachable
        )
