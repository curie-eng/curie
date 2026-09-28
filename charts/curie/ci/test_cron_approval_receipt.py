"""Check the isolated receipt fixture's Slack response and card marker."""

from __future__ import annotations

import hashlib
import http.client
import importlib.util
import json
import runpy
import sys
import threading
from http.server import ThreadingHTTPServer
from pathlib import Path
from unittest.mock import patch

_root = Path(__file__).parents[3]
_receipt_path = _root / "cli/scripts/fixtures/mcp-receipt/server.py"
_spec = importlib.util.spec_from_file_location("mcp_receipt", _receipt_path)
assert _spec is not None and _spec.loader is not None
_receipt = importlib.util.module_from_spec(_spec)
sys.modules["mcp_receipt"] = _receipt
_spec.loader.exec_module(_receipt)
_fixture = runpy.run_path(str(Path(__file__).with_name("cron-approval-receipt.py")))
Handler = _fixture["Handler"]
CARD_MARKER = _fixture["CARD_MARKER"]


def _request(port: int, path: str, payload: dict) -> dict:
    connection = http.client.HTTPConnection("127.0.0.1", port, timeout=3)
    connection.request(
        "POST",
        path,
        body=json.dumps(payload),
        headers={"Content-Type": "application/json"},
    )
    response = connection.getresponse()
    assert response.status == 200
    body = json.loads(response.read())
    connection.close()
    return body


def test_card_marker_only_for_approval_buttons() -> None:
    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        port = server.server_port
        ordinary = {"channel": "C0EXAMPLE1", "text": "hello"}
        card = {
            "channel": "C0EXAMPLE1",
            "text": "Approval required: receipt_read",
            "blocks": [
                {
                    "type": "actions",
                    "elements": [
                        {
                            "type": "button",
                            "action_id": "curie-approval-approve",
                            "value": "approval-example",
                        },
                        {
                            "type": "button",
                            "action_id": "curie-approval-reject",
                            "value": "approval-example",
                        },
                    ],
                }
            ],
        }
        with patch("builtins.print") as marker:
            posted = _request(port, "/chat.postMessage", ordinary)
            assert posted["ok"] is True
            assert posted["channel"] == ordinary["channel"]
            assert isinstance(posted["ts"], str)
            _request(port, "/chat.update", {**ordinary, "ts": posted["ts"]})
            assert marker.call_count == 0
            card_response = _request(port, "/chat.postMessage", card)
            assert card_response["ok"] is True
            _request(port, "/chat.postMessage", {**card, "channel": "C0EXAMPLE2"})
            digest = hashlib.sha256(b"approval-example").hexdigest()
            marker.assert_called_once_with(f"{CARD_MARKER} {digest}", flush=True)
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=3)


def test_wrong_approval_buttons_do_not_count() -> None:
    approval_card_id = _fixture["_approval_card_id"]
    assert (
        approval_card_id({"channel": "C0EXAMPLE1", "blocks": [{"type": "actions", "elements": []}]})
        is None
    )
    assert (
        approval_card_id(
            {
                "channel": "C0EXAMPLE1",
                "blocks": [
                    {
                        "type": "actions",
                        "elements": [
                            {
                                "type": "button",
                                "action_id": "curie-approval-approve",
                                "value": "one",
                            },
                            {
                                "type": "button",
                                "action_id": "curie-approval-reject",
                                "value": "two",
                            },
                        ],
                    }
                ],
            }
        )
        is None
    )


def test_mcp_initialize_uses_receipt_handler() -> None:
    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        connection = http.client.HTTPConnection("127.0.0.1", server.server_port, timeout=3)
        connection.request(
            "POST",
            "/mcp",
            body=json.dumps(
                {
                    "jsonrpc": "2.0",
                    "id": 1,
                    "method": "initialize",
                    "params": {"protocolVersion": "2025-03-26"},
                }
            ),
            headers={
                "Content-Type": "application/json",
                "Accept": "application/json,text/event-stream",
            },
        )
        response = connection.getresponse()
        assert response.status == 200
        assert response.getheader("Mcp-Session-Id")
        assert json.loads(response.read())["result"]["serverInfo"]["name"] == "curie-mcp-receipt"
        connection.close()
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=3)
