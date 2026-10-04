"""The single consumer group creation path used by platform services."""

from collections.abc import Awaitable
from typing import Any, Protocol

from redis.exceptions import ResponseError


class GroupBroker(Protocol):
    """Only the broker operation required to create a consumer group."""

    def xgroup_create(
        self, name: Any, groupname: Any, id: Any = ..., mkstream: bool = ...
    ) -> Awaitable[Any]: ...


async def ensure_group(
    broker: GroupBroker, stream: str, group: str, *, start_id: str
) -> None:
    """Create an absent stream and group at the caller's explicit position.

    An existing group keeps its position. Only BUSYGROUP is suppressed; other
    server failures propagate so an enqueue cannot claim successful setup.
    """
    try:
        await broker.xgroup_create(stream, group, id=start_id, mkstream=True)
    except ResponseError as exc:
        if "BUSYGROUP" not in str(exc):
            raise
