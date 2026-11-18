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
        user = await self._read(
            "users.info", params={"user": user_id}, headers=headers, field="user", expected=user_id
        )
        team = await self._read(
            "team.info",
            params={"team": workspace},
            headers=headers,
            field="team",
            expected=workspace,
        )
        profile = user.get("profile") if user else None
        return AccountProfile(
            display_name=(
                _label(profile.get("display_name")) if isinstance(profile, dict) else None
            )
            or (_label(user.get("real_name")) if user else None),
            handle=_label(user.get("name")) if user else None,
            workspace_name=_label(team.get("name")) if team else None,
        )

    async def _read(
        self,
        method: str,
        *,
        params: dict[str, str],
        headers: dict[str, str],
        field: str,
        expected: str,
    ) -> dict[str, object] | None:
        try:
            response = await self._http.get(
                f"{self._base}/{method}", params=params, headers=headers, timeout=5
            )
            response.raise_for_status()
            body = response.json()
        except (httpx.HTTPError, ValueError):
            return None
        if not isinstance(body, dict) or body.get("ok") is not True:
            return None
        value = body.get(field)
        return value if isinstance(value, dict) and value.get("id") == expected else None


def _label(value: object) -> str | None:
    return value[:256] if isinstance(value, str) and value.strip() else None
