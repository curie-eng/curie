"""Shared in-process HTTP capture server for reply-sink and kernel wire tests.

Nothing internal is mocked: adapters talk HTTP to this aiohttp app so "which
secret reached which URL" is read off the bytes that actually left the process.
"""

from __future__ import annotations

import json
from typing import Any

from aiohttp import web

# EB-B4: the per-adapter egress credential travels in this header, and only this
# header. Pinned as a constant so a rename shows up as one failure, not thirty.
SECRET_HEADER = "X-Curie-Adapter-Secret"


class Capture:
    """Records every request that reaches it, whatever the route.

    ``/a`` and ``/b`` are two distinct adapter endpoints; ``/slack/api/`` is the
    configured Slack origin and ``/slack/dead/`` is a SAME-ORIGIN path whose
    connection drops, which is the only shape that can still exercise #530's
    transport fallback once D4.4 refuses cross-origin endpoints.
    """

    def __init__(self) -> None:
        self.requests: list[dict[str, Any]] = []
        self.redirect_cluster_message_replies = False
        self.app = web.Application()
        self.app.add_routes(
            [
                web.post("/a", self._record_ok),
                web.post("/b", self._record_ok),
                web.post(
                    "/v1/internal/cluster-message-replies/{reply_ref}",
                    self._record_cluster_message_reply,
                ),
                web.post("/slack/api/{method}", self._record_slack),
                web.post("/slack/dead/{method}", self._drop),
                web.post("/redirect", self._redirect_to_b),
            ]
        )

    async def _capture(self, request: web.Request) -> None:
        self.requests.append(
            {
                "path": request.path,
                "headers": dict(request.headers),
                "body": await request.text(),
            }
        )

    async def _record_ok(self, request: web.Request) -> web.Response:
        await self._capture(request)
        # ``ref`` is the adapter-minted handle a ``reply.post`` ack carries back
        # (EB-B1's ``ReplyAck``); an adapter with nothing to mint omits it.
        return web.json_response({"ref": "msg_minted"})

    async def _record_slack(self, request: web.Request) -> web.Response:
        await self._capture(request)
        return web.json_response({"ok": True, "ts": "1720000000.000200"})

    async def _record_cluster_message_reply(self, request: web.Request) -> web.Response:
        await self._capture(request)
        if self.redirect_cluster_message_replies:
            return web.Response(status=307, headers={"Location": "/b"})
        return web.json_response({"ref": request.match_info["reply_ref"]})

    async def _drop(self, request: web.Request) -> web.Response:
        # Record it (so a test can prove the call was ATTEMPTED here) and then
        # kill the connection, which the client sees as an aiohttp.ClientError --
        # the "unreachable" class #530's fallback keys on, as distinct from a
        # SlackApiError, which means the endpoint answered.
        await self._capture(request)
        transport = request.transport
        assert transport is not None
        transport.close()
        return web.Response()

    async def _redirect_to_b(self, request: web.Request) -> web.Response:
        # A redirecting adapter endpoint: record the attempt, then point the
        # client at ANOTHER path. aiohttp's default would replay the POST there
        # with the egress secret still attached, so ``/b`` receiving anything is
        # the leak this shape exists to catch.
        await self._capture(request)
        return web.Response(status=307, headers={"Location": "/b"})

    def methods(self) -> list[str]:
        return [r["path"].rsplit("/", 1)[-1] for r in self.requests]

    def paths(self) -> list[str]:
        return [r["path"] for r in self.requests]

    def secrets(self) -> list[str | None]:
        return [r["headers"].get(SECRET_HEADER) for r in self.requests]

    def bodies_mentioning(self, needle: str) -> list[dict[str, Any]]:
        return [
            r for r in self.requests if needle in r["body"] or needle in json.dumps(r["headers"])
        ]
