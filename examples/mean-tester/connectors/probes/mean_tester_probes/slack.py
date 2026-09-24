"""The four Slack Web API calls the tester makes, and nothing else.

Scopes: chat:write, channels:read, channels:history, groups:history. These are
the platform app's own (apps/dispatcher/slack-app-manifest.yaml).
"""

import httpx

BASE = "https://slack.com/api/"


class SlackError(RuntimeError):
    pass


class SlackApi:
    def __init__(self, token: str, client: httpx.Client | None = None) -> None:
        self._client = client or httpx.Client(timeout=30)
        self._headers = {"Authorization": f"Bearer {token}"}

    def _call(self, method: str, *, params: dict | None = None, json: dict | None = None) -> dict:
        if json is not None:
            r = self._client.post(BASE + method, headers=self._headers, json=json)
        else:
            r = self._client.get(BASE + method, headers=self._headers, params=params)
        r.raise_for_status()
        body = r.json()
        if not body.get("ok"):
            raise SlackError(f"{method} refused: {body.get('error', 'unknown')}")
        return body

    def post(self, channel: str, text: str) -> str:
        return self._call("chat.postMessage", json={"channel": channel, "text": text})["ts"]

    def replies(self, channel: str, ts: str) -> list[dict]:
        params = {"channel": channel, "ts": ts, "limit": 50}
        return self._call("conversations.replies", params=params)["messages"]

    def channel_info(self, channel: str) -> dict:
        return self._call("conversations.info", params={"channel": channel})["channel"]

    def members(self, channel: str) -> set[str]:
        params = {"channel": channel, "limit": 1000}
        return set(self._call("conversations.members", params=params)["members"])
