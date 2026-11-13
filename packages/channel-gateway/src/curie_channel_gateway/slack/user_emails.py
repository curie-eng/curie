"""The Slack adapter behind the user-email port (@spec GATEWAY-USER-EMAIL-2, -3).

An external HTTP identity provider maps a Slack sender to a directory account by
email. The gateway keeps the Slack token, so it answers that one question:
``users.info`` read fresh with the named identity's bot token, every call. It
caches no email: an identity decision must see a deactivation as soon as Slack
does, and ``users.info`` is a Tier 4 method with ample headroom.

Every failure is an exception with fixed text. Nothing here logs; an email, a
user ID or a Slack body never reaches a log line or an error message.
"""

from __future__ import annotations

import asyncio
from collections.abc import Mapping
from typing import Any

import httpx

from ..ports.reads import UserEmailSource
from ..user_email_models import UserEmailLookupError, UserEmailUnknown

_UNKNOWN_ERRORS = frozenset({"user_not_found", "user_not_visible", "users_not_found"})


class SlackUserEmails:
    """Reads one Slack user's email with the gateway-held bot token."""

    def __init__(
        self,
        http: httpx.AsyncClient,
        tokens: Mapping[str, str],
        *,
        base_url: str | None = None,
        timeout_s: float = 5.0,
    ) -> None:
        if timeout_s <= 0:
            raise ValueError("user email timeout must be positive")
        self._http = http
        self._tokens = {name: token for name, token in tokens.items() if token.strip()}
        self._endpoint = (base_url or "https://slack.com/api/").rstrip("/") + "/users.info"
        self._timeout_s = timeout_s

    async def email_of(self, *, identity: str, user_id: str, workspace: str) -> str:
        token = self._tokens.get(identity)
        if token is None:
            raise UserEmailLookupError("gateway identity unavailable")
        try:
            async with asyncio.timeout(self._timeout_s):
                response = await self._http.get(
                    self._endpoint,
                    params={"user": user_id},
                    headers={"Authorization": f"Bearer {token}"},
                )
            response.raise_for_status()
            body = response.json()
        except httpx.HTTPError:
            raise UserEmailLookupError("slack user lookup failed") from None
        except ValueError:  # a 200 that is not JSON at all
            raise UserEmailLookupError("slack user lookup returned a non-JSON body") from None
        if not isinstance(body, dict):
            raise UserEmailLookupError("slack user lookup returned an invalid body")
        if body.get("ok") is not True:
            if body.get("error") in _UNKNOWN_ERRORS:
                raise UserEmailUnknown("slack user unknown")
            raise UserEmailLookupError("slack user lookup refused")
        user = body.get("user")
        if not isinstance(user, dict):
            raise UserEmailLookupError("slack user lookup returned no user")
        return _usable_email(user, user_id, workspace)


def _usable_email(user: Mapping[str, Any], user_id: str, workspace: str) -> str:
    if user.get("id") != user_id or not _in_workspace(user, workspace):
        raise UserEmailUnknown("slack user unknown")
    if user.get("deleted") is not False or user.get("is_bot") or user.get("is_app_user"):
        raise UserEmailUnknown("slack user unknown")
    profile = user.get("profile")
    email = profile.get("email") if isinstance(profile, dict) else None
    if not isinstance(email, str) or not email.strip():
        raise UserEmailUnknown("slack user unknown")
    return email


def _in_workspace(user: Mapping[str, Any], workspace: str) -> bool:
    if user.get("team_id") == workspace:
        return True
    enterprise = user.get("enterprise_user")
    if not isinstance(enterprise, dict):
        return False
    teams = enterprise.get("teams")
    return enterprise.get("enterprise_id") == workspace or (
        isinstance(teams, list) and workspace in teams
    )


_CONFORMS: type[UserEmailSource] = SlackUserEmails
