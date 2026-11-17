"""Display labels for a Slack account, using only gateway-held credentials."""

from collections.abc import Mapping

import httpx

from ..read_models import AccountProfile


class SlackAccountProfiles:
    def __init__(
        self, http: httpx.AsyncClient, tokens: Mapping[str, str], *, base_url: str | None = None
    ) -> None:
        self._http = http
        self._tokens = tokens
        self._base = (base_url or "https://slack.com/api/").rstrip("/")

    async def profile_of(self, *, identity: str, user_id: str, workspace: str) -> AccountProfile:
        token = self._tokens.get(identity)
        if not token:
            raise ValueError("account profile unavailable")
        headers = {"Authorization": f"Bearer {token}"}
        user_response = await self._http.get(
            f"{self._base}/users.info", params={"user": user_id}, headers=headers, timeout=5
        )
        team_response = await self._http.get(
            f"{self._base}/team.info", params={"team": workspace}, headers=headers, timeout=5
        )
        user_response.raise_for_status()
        team_response.raise_for_status()
        user_body, team_body = user_response.json(), team_response.json()
        if not isinstance(user_body, dict) or not isinstance(team_body, dict):
            raise ValueError("account profile unavailable")
        user, team = user_body.get("user"), team_body.get("team")
        if (
            user_body.get("ok") is not True
            or team_body.get("ok") is not True
            or not isinstance(user, dict)
            or not isinstance(team, dict)
            or user.get("id") != user_id
            or team.get("id") != workspace
        ):
            raise ValueError("account profile unavailable")
        profile = user.get("profile")
        if not isinstance(profile, dict):
            raise ValueError("account profile unavailable")
        return AccountProfile(
            display_name=_label(profile.get("display_name")) or _label(user.get("real_name")),
            handle=_label(user.get("name")),
            workspace_name=_label(team.get("name")),
        )


def _label(value: object) -> str | None:
    return value[:256] if isinstance(value, str) and value.strip() else None
