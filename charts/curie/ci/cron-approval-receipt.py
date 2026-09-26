#!/usr/bin/env python3
"""MCP receipt and isolated Slack Web API fixture for the cron approval proof.

Only accepted approval card posts produce a Slack marker. The fixture never logs
request bodies, headers, tokens, channel names, or approval identifiers.
"""

from __future__ import annotations

import hashlib
import itertools
import json
import threading
from http import HTTPStatus
from http.server import ThreadingHTTPServer
from typing import Any

from mcp_receipt import MAX_REQUEST_BYTES
from mcp_receipt import Handler as ReceiptHandler

CARD_MARKER = "SLACK_APPROVAL_CARD"
_ACTION_PAIRS = (
    frozenset({"curie-approval-approve", "curie-approval-reject"}),
    frozenset({"curie-approval-approve-note", "curie-approval-reject-note"}),
)
_next_message = itertools.count(1)
_message_lock = threading.Lock()


def _approval_card_id(payload: dict[str, Any]) -> str | None:
    if payload.get("channel") != "C0EXAMPLE1":
        return None
    blocks = payload.get("blocks")
    if not isinstance(blocks, list):
        return None
    for block in blocks:
        if not isinstance(block, dict) or block.get("type") != "actions":
            continue
        elements = block.get("elements")
        if not isinstance(elements, list) or len(elements) != 2:
            continue
        if not all(isinstance(item, dict) and item.get("type") == "button" for item in elements):
            continue
        if not all(isinstance(item.get("action_id"), str) for item in elements):
            continue
        action_ids = frozenset(item["action_id"] for item in elements)
        values = [item.get("value") for item in elements]
        if (
            action_ids in _ACTION_PAIRS
            and all(isinstance(value, str) and value for value in values)
            and values[0] == values[1]
        ):
            return values[0]
    return None


class Handler(ReceiptHandler):
    def do_POST(self) -> None:  # noqa: N802
        if self.path not in {"/chat.postMessage", "/chat.update"}:
            super().do_POST()
            return
        if self.headers.get("Content-Type", "").split(";", 1)[0].strip() != "application/json":
            self._send_json(
                HTTPStatus.UNSUPPORTED_MEDIA_TYPE,
                {"ok": False, "error": "invalid_request"},
            )
            self.close_connection = True
            return
        try:
            length = int(self.headers.get("Content-Length", "0"))
            if length <= 0 or length > MAX_REQUEST_BYTES:
                raise ValueError("invalid request size")
            payload = json.loads(self.rfile.read(length))
            if not isinstance(payload, dict):
                raise ValueError("expected an object")
            channel = payload.get("channel")
            if not isinstance(channel, str) or not channel:
                raise ValueError("missing channel")
            if self.path.endswith("chat.update"):
                ts = payload.get("ts")
                if not isinstance(ts, str) or not ts:
                    raise ValueError("missing timestamp")
            else:
                with _message_lock:
                    ts = f"1700000000.{next(_next_message):06d}"
        except (TypeError, ValueError):
            self._send_json(HTTPStatus.BAD_REQUEST, {"ok": False, "error": "invalid_request"})
            self.close_connection = True
            return

        approval_id = _approval_card_id(payload) if self.path == "/chat.postMessage" else None
        if approval_id is not None:
            print(f"{CARD_MARKER} {hashlib.sha256(approval_id.encode()).hexdigest()}", flush=True)
        self._send_json(HTTPStatus.OK, {"ok": True, "channel": channel, "ts": ts})


if __name__ == "__main__":
    ThreadingHTTPServer(("0.0.0.0", 8000), Handler).serve_forever()
