"""Authenticated delivery of the existing channel reply wire."""

from typing import Protocol

from channel_protocol import ReplyAck, ReplyEvent


class AuthenticatedReplyTransport[Route](Protocol):
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
