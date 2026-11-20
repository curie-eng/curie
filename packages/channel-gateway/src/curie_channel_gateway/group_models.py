"""Provider membership evidence consumed by API authorization policy."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime


class UserGroupLookupError(Exception):
    """A usergroup lookup that did not produce a member set.

    The authorizer maps this to a fail-closed denial, so it is raised for every
    failure mode rather than returning an empty set: "nobody is in this group"
    and "we could not find out" are different facts and must stay different.
    """


@dataclass(frozen=True)
class UserGroupMembership:
    """A group's member set plus the provenance the audit row records: when the
    membership was fetched, and how stale the answer that decided is."""

    group: str
    users: frozenset[str]
    fetched_at: datetime
    cache_age_s: float
