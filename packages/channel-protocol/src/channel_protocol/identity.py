"""Canonical internal identities for channel conversations."""

from urllib.parse import quote


def scoped_conversation_id(kind: str, address: str, conversation_id: str) -> str:
    """Return the collision-free internal identity for a routed conversation.

    Channel-native conversation ids are opaque and unique only within their
    adapter address. Encoding every component before joining keeps component
    boundaries unambiguous without imposing parsing rules on adapters.
    """

    return ":".join(
        quote(component, safe="")
        for component in (kind, address, conversation_id)
    )
