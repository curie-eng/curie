"""Group membership data, with an explicit uncached authorization read."""

from typing import Protocol


class GroupMembershipSource[Membership](Protocol):
    """Membership is the API's existing UserGroupMembership value type.

    Both methods raise the existing UserGroupLookupError when no answer can be
    obtained; an empty member set is a successful read. fresh_members bypasses
    cached and already in-flight cached reads, fetches the provider now and
    returns cache_age_s=0. The API still decides membership and authorization.
    """

    async def members(self, group_id: str) -> Membership: ...

    async def fresh_members(self, group_id: str) -> Membership: ...
