"""Canonical internal identities for channel conversations."""

import uuid
from urllib.parse import quote

#: The identity whose thread key carries no identity segment: the one Slack app
#: every route had before ADR-0168. A copy of ``aci_protocol.turn.DEFAULT_IDENTITY``,
#: because this package does not depend on that one; ``test_identity`` pins them.
DEFAULT_IDENTITY = "default"


def scoped_conversation_id(
    kind: str,
    address: str,
    conversation_id: str,
    *,
    identity: str | None = None,
) -> str:
    """Return the collision-free internal identity for a routed conversation.

    Channel-native conversation ids are opaque and unique only within their
    adapter address. Encoding every component before joining keeps component
    boundaries unambiguous without imposing parsing rules on adapters.

    ``identity`` is the route's resolved identity (``route_identity``), a
    segment after ``kind`` unless it is None or ``DEFAULT_IDENTITY``
    (ADR-0168 decision 4).
    """

    components = (
        (kind, address, conversation_id)
        if identity is None or identity == DEFAULT_IDENTITY
        else (kind, identity, address, conversation_id)
    )
    return ":".join(quote(component, safe="") for component in components)


def hook_conversation_id(
    agent_id: uuid.UUID, hook: str, partition: str | None = None
) -> str:
    """The thread a hook delivery lands on.

    Per HOOK by default rather than per delivery, and that choice is load-bearing
    in two directions. Per delivery would claim a fresh sandbox for every event,
    and two rapid firings would run concurrently with no ordering at all. Sharing
    one thread instead means a hook reuses its session and a second firing
    arriving mid-run defers until the first finishes, which is exactly ADR-0079's
    "jobs are outputs, not steering inputs" applied to a hook competing with
    itself.

    ADR-0134 narrows that to per PARTITION where the operator asks for it. A
    partition is a thread with the lifetime a Slack thread ts has: the deliveries
    about one pull request still serialize against each other, while deliveries
    about different pull requests no longer do. Which is why a partition value
    must be a stable identity of the thing and never a run id or a timestamp.

    The three-segment prefix is preserved verbatim under a partition, so a
    partitioned id is still disjoint from the agent's Slack thread ids and a hook
    can never land in the middle of a human conversation.

    Args:
        agent_id: The agent this hook belongs to.
        hook: The validated hook name.
        partition: The derived partition value, or None for the unpartitioned id.

    Returns:
        The conversation key.
    """

    unpartitioned = f"hook:{agent_id}:{hook}"
    if partition is None:
        return unpartitioned
    return f"{unpartitioned}:{partition}"
