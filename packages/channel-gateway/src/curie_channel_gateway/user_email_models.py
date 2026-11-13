"""Slack user email evidence shared by the gateway and identity-provider callers."""

from __future__ import annotations


class UserEmailLookupError(Exception):
    """A lookup that produced no email; callers must refuse, never default.

    Messages are fixed text: an email, a user ID or a provider body never
    appears in one.
    """


class UserEmailUnknown(UserEmailLookupError):
    """The provider answered, and this user has no usable email in the workspace.

    Raised for an unknown or invisible user, a workspace mismatch, a deleted,
    bot or app user, and a user without an email. Distinct from an outage so a
    provider can answer ``unresolved`` rather than ``unavailable``.
    """
