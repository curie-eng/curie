"""Whose site and whose token a Jira binding talks to (#4358 decision 3).

The site's ``/_edge/tenant_info`` names its cloudId and ``/rest/api/3/myself``
names the token's accountId. Both are read once per connection and cached; every
binding then checks them against its own scope and the tracker's ``mention``.
The tracker checks before its first poll and before every write, and the status
comments check before they select a comment as the bot's own, so a token that is
not the configured bot can neither split a ticket into two WorkItems nor edit a
person's comment that happens to carry the marker.
"""

from __future__ import annotations

from dataclasses import dataclass
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from curie_api.forges.errors import NotFound, Unauthorized, Unavailable
from curie_api.forges.jira_cloud.transport import JiraTransport


@dataclass
class IdentityCache:
    """What the site and the token say they are, read once per connection."""

    cloud_id: str | None = None
    account_id: str | None = None
    time_zone: str = "UTC"


def _zone(name: str) -> ZoneInfo:
    try:
        return ZoneInfo(name)
    except (ZoneInfoNotFoundError, ValueError):
        return ZoneInfo("UTC")


async def verify_identity(
    transport: JiraTransport,
    cache: IdentityCache,
    *,
    site_origin: str,
    scope_id: str,
    mention: str,
) -> ZoneInfo:
    """The bot's time zone once the site is ``scope_id`` and the token is ``mention``.

    `Unauthorized` names which one differs, never either value.
    """

    if cache.cloud_id is None or cache.account_id is None:
        try:
            tenant = await transport.anonymous_get(
                f"{site_origin}/_edge/tenant_info", what="tenant_info"
            )
        except NotFound:
            raise Unavailable("tenant_info") from None
        # https://developer.atlassian.com/cloud/jira/platform/rest/v3/api-group-myself/#api-rest-api-3-myself-get
        myself = await transport.get("/myself", what="myself")
        cloud_id = tenant.get("cloudId") if isinstance(tenant, dict) else None
        account_id = myself.get("accountId") if isinstance(myself, dict) else None
        if not isinstance(cloud_id, str) or not isinstance(account_id, str):
            raise Unavailable("identity")
        zone = myself.get("timeZone")
        cache.time_zone = zone if isinstance(zone, str) and zone else "UTC"
        cache.cloud_id, cache.account_id = cloud_id, account_id
    if cache.cloud_id != scope_id:
        raise Unauthorized("cloud id mismatch: the site is not the binding's tracker scope")
    if cache.account_id != mention:
        raise Unauthorized("bot account mismatch: the token is not the tracker's mention")
    return _zone(cache.time_zone)
