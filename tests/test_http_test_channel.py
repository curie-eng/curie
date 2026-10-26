"""Unit tests for tests/e2e/http_test_channel.py (pure local, no stack)."""

from __future__ import annotations

import importlib.util
import json
import threading
import time
import urllib.error
import urllib.request
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

import pytest

_SPEC = importlib.util.spec_from_file_location(
    "http_test_channel", Path(__file__).parent / "e2e" / "http_test_channel.py"
)
assert _SPEC and _SPEC.loader
htc = importlib.util.module_from_spec(_SPEC)
_SPEC.loader.exec_module(htc)

SECRET = "unit-secret"


def _post(port: int, body: dict, secret: str | None):
    req = urllib.request.Request(
        f"http://127.0.0.1:{port}/", data=json.dumps(body).encode(), method="POST"
    )
    req.add_header("Content-Type", "application/json")
    if secret is not None:
        req.add_header(htc.ADAPTER_SECRET_HEADER, secret)
    try:
        with urllib.request.urlopen(req, timeout=5) as resp:
            return resp.status, json.loads(resp.read())
    except urllib.error.HTTPError as exc:
        return exc.code, None


@pytest.fixture
def server(tmp_path):
    db = str(tmp_path / "d.db")
    with htc.TestChannelServer("127.0.0.1", 0, SECRET, db) as srv:
        yield srv, db


def _reply(text: str, conv: str = "c1") -> dict:
    return {
        "version": "1.0",
        "event": "reply.post",
        "target": {"conversation_id": conv, "reply_ref": "r"},
        "message": {"text": text},
        "requested_by": "u",
    }


def test_stores_and_waits(server):
    srv, db = server
    since = htc.utc_now()
    status, ack = _post(srv.port, _reply("hello scheduled world"), SECRET)
    assert status == 200 and ack["ref"]
    hit = htc.wait_for_delivery(db, since, contains="scheduled", timeout=3, interval=0.05)
    assert hit["text"] == "hello scheduled world"
    assert hit["event"] == "reply.post"
    assert hit["conversation_id"] == "c1"
    assert hit["raw"]["requested_by"] == "u"
    assert htc.wait_for_delivery(db, since, contains="absent", timeout=0.2, interval=0.05) is None
    assert htc.wait_for_delivery(db, since, conversation="other", timeout=0.2) is None


def test_wrong_secret_rejected(server):
    srv, db = server
    assert _post(srv.port, _reply("x"), "bad")[0] == 401
    assert _post(srv.port, _reply("x"), None)[0] == 401
    assert htc.count_deliveries(db, "1970-01-01T00:00:00Z") == 0


def test_count_since_until(server):
    srv, db = server
    _post(srv.port, _reply("one"), SECRET)
    time.sleep(0.05)
    mid = htc.utc_now()
    time.sleep(0.05)
    _post(srv.port, _reply("two"), SECRET)
    epoch = "1970-01-01T00:00:00Z"
    assert htc.count_deliveries(db, epoch) == 2
    assert htc.count_deliveries(db, mid) == 1
    assert htc.count_deliveries(db, epoch, until=mid) == 1
    assert htc.count_deliveries(db, epoch, contains="two") == 1


def test_send_turn_against_fake_api():
    seen: list[tuple[str, dict, str | None]] = []

    class Fake(BaseHTTPRequestHandler):
        def log_message(self, *a):
            pass

        def do_POST(self):
            body = json.loads(self.rfile.read(int(self.headers["Content-Length"])))
            seen.append((self.path, body, self.headers.get("X-API-Key")))
            out = (
                {"token": "minted"}
                if self.path == "/channels/token"
                else {"event_id": "evt-1", "stream_id": "s", "duplicate": False}
            )
            data = json.dumps(out).encode()
            self.send_response(200)
            self.send_header("Content-Length", str(len(data)))
            self.end_headers()
            self.wfile.write(data)

    api = ThreadingHTTPServer(("127.0.0.1", 0), Fake)
    threading.Thread(target=api.serve_forever, daemon=True).start()
    try:
        result = htc.send_turn(
            f"http://127.0.0.1:{api.server_address[1]}",
            "platform-key",
            "agent-1",
            "addr-1",
            "test-adapter",
            "do the thing",
            conversation_id="conv-9",
            author="tester",
        )
    finally:
        api.shutdown()
        api.server_close()
    assert result == {"event_id": "evt-1", "conversation_id": "conv-9"}
    (tpath, tbody, tkey), (ipath, ibody, ikey) = seen
    assert tpath == "/channels/token" and tkey == "platform-key"
    assert tbody["adapter"] == "test-adapter" and tbody["address"] == "addr-1"
    assert ipath == "/channels/turns" and ikey == "minted"
    assert ibody["conversation_id"] == "conv-9"
    assert ibody["author"] == "tester" and ibody["text"] == "do the thing"
    assert ibody["kind"] == "http" and ibody["address"] == "addr-1"
    assert ibody["delivery_id"] and ibody["reply_ref"]
