"""Request ids and lock keys derived from GitHub identifiers."""

import hashlib
import uuid

from fastapi import HTTPException


def delivery_uuid(delivery_id: str) -> uuid.UUID:
    try:
        delivery = uuid.UUID(delivery_id)
    except (ValueError, TypeError, AttributeError):
        raise HTTPException(400, {"code": "invalid_delivery"}) from None
    if str(delivery) != delivery_id.lower():
        raise HTTPException(400, {"code": "invalid_delivery"}) from None
    return delivery


def issue_lock_keys_for(repository_id: int, issue_number: int) -> tuple[int, int]:
    digest = hashlib.sha256(f"curie-factory:{repository_id}:{issue_number}".encode()).digest()
    return (
        int.from_bytes(digest[:4], "big", signed=True),
        int.from_bytes(digest[4:8], "big", signed=True),
    )


def label_event_delivery_id(repository_id: int, issue_number: int, event_id: int) -> uuid.UUID:
    """A stable stand-in delivery id for one labeled event.

    It never collides with a real X-GitHub-Delivery, and the same event always
    yields the same request id.
    """

    return uuid.uuid5(
        uuid.NAMESPACE_URL,
        f"https://github.com/factory/reconcile/{repository_id}/{issue_number}/{event_id}",
    )
