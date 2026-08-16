"""The AgentMail REST client: the adapter's only outbound provider surface.

Four calls, all stdlib ``urllib``. Every method returns ``(status, parsed)`` and
never raises for a transport failure, which is reported as status ``0`` so the
caller can tell "the provider said no" from "the provider was unreachable".
Bearer auth is on every call (https://docs.agentmail.to/api-reference/overview).
"""

from __future__ import annotations

import json
import urllib.error
import urllib.parse
import urllib.request
from typing import Any

from .config import MailAdapterConfig

HTTP_TIMEOUT_SECONDS = 30.0

# The categories AgentMail withholds from List Messages results unless the caller
# asks for them by name. Sending them explicitly changes nothing about what a
# correct provider returns today, and that is the point: a provider that changes
# a default, or a key that carries the label-read permissions, cannot silently
# widen what reaches the agent. Parameter names and their "Include <category> in
# results" semantics are from
# https://docs.agentmail.to/api-reference/inboxes/messages/list ; the documented
# default exclusion they restate is from https://www.agentmail.to/docs/messages .
# These are constants, not parameters and not config: no caller can turn them on.
EXCLUDED_CATEGORIES = {
    "include_spam": "false",
    "include_blocked": "false",
    "include_unauthenticated": "false",
}


class _NoRedirectHandler(urllib.request.HTTPRedirectHandler):
    """Refuse every 3xx instead of following it.

    `docs/guides/building-a-channel-adapter.md` section 4: "Never redirect. A 3xx
    is treated as a delivery failure and is not followed, because following it
    would replay the egress secret at whatever origin the redirect named."
    `urlopen` does the opposite by default, rebuilding the request for the new URL
    with every header the caller added, so one 302 from a compromised or
    misconfigured origin hands out `AGENTMAIL_API_KEY` or `CURIE_CHANNEL_TOKEN`.
    Returning None here declines the redirect, and the opener chain then raises
    the 3xx as an `HTTPError`, which `request` reports as an ordinary failure.
    """

    def redirect_request(self, *args: Any, **kwargs: Any) -> None:
        return None


_OPENER = urllib.request.build_opener(_NoRedirectHandler)


def request(
    method: str, url: str, body: dict[str, Any] | None = None, headers: dict[str, str] | None = None
) -> tuple[int, Any]:
    """One HTTP round trip. Returns (status, parsed body); status 0 is transport failure.

    A 3xx is never followed; it comes back as its own status, which every caller
    already treats as a failure.
    """
    data = json.dumps(body).encode() if body is not None else None
    req = urllib.request.Request(url, data=data, method=method)
    req.add_header("Content-Type", "application/json")
    for key, value in (headers or {}).items():
        req.add_header(key, value)
    try:
        with _OPENER.open(req, timeout=HTTP_TIMEOUT_SECONDS) as response:
            raw = response.read().decode()
            status = int(response.status)
    except urllib.error.HTTPError as exc:
        raw = exc.read().decode()
        status = int(exc.code)
    except OSError as exc:
        return 0, {"error": str(exc)}
    try:
        return status, json.loads(raw)
    except ValueError:
        return status, raw


def _quoted(value: str) -> str:
    return urllib.parse.quote(value, safe="")


class AgentMailClient:
    """The provider seam. Holds the config; owns no state of its own."""

    def __init__(self, config: MailAdapterConfig) -> None:
        self.config = config

    def _call(self, method: str, path: str, body: dict[str, Any] | None = None) -> tuple[int, Any]:
        return request(
            method,
            f"{self.config.agentmail_base_url.rstrip('/')}{path}",
            body,
            {"Authorization": f"Bearer {self.config.agentmail_api_key}"},
        )

    @property
    def _inbox(self) -> str:
        return _quoted(self.config.agentmail_inbox)

    def list_messages(self, limit: int, page_token: str | None = None) -> tuple[int, Any]:
        """List the inbox, restating the provider's default exclusions explicitly.

        `page_token` walks the listing the provider pages with; the envelope's
        `next_page_token` is the value to pass back.
        https://docs.agentmail.to/api-reference/inboxes/messages/list
        """
        params: dict[str, Any] = {"limit": limit, **EXCLUDED_CATEGORIES}
        if page_token:
            params["page_token"] = page_token
        query = urllib.parse.urlencode(params)
        return self._call("GET", f"/inboxes/{self._inbox}/messages?{query}")

    def get_message(self, message_id: str) -> tuple[int, Any]:
        return self._call("GET", f"/inboxes/{self._inbox}/messages/{_quoted(message_id)}")

    def get_thread(self, thread_id: str) -> tuple[int, Any]:
        return self._call("GET", f"/inboxes/{self._inbox}/threads/{_quoted(thread_id)}")

    def reply(self, message_id: str, text: str) -> tuple[int, Any]:
        return self._call(
            "POST",
            f"/inboxes/{self._inbox}/messages/{_quoted(message_id)}/reply",
            {"text": text},
        )
