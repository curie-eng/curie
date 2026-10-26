"""A persistent generic HTTP channel for end-to-end proofs.

Purpose: let a proof observe deliveries that happen minutes after the turn that
caused them (scheduled runs, delayed replies). The channel has two halves.

Inbound: ``send_turn`` mints a channel token (``POST /channels/token``) and
enqueues a turn (``POST /channels/turns``) on a binding of kind ``http``.

Outbound: the worker's HTTP reply adapter POSTs reply events (``reply.post``,
``reply.update``, ``turn.status``, ``turn.completed``) to the binding's
endpoint with the adapter secret header. ``TestChannelServer`` is that
endpoint. It stores every authenticated event in SQLite and answers with the
``{"ref": ...}`` acknowledgement the adapter expects. A wrong or missing secret
gets 401 and is not stored.

Reuse from another proof::

    from tests.e2e.http_test_channel import (
        TestChannelServer, bind_channel, send_turn, wait_for_delivery,
        count_deliveries,
    )

    with TestChannelServer("0.0.0.0", 8099, secret, "deliveries.db") as srv:
        bind_channel(api_url, key, agent_id, address, endpoint, adapter)
        sent = send_turn(api_url, key, agent_id, address, adapter, "hello")
        hit = wait_for_delivery("deliveries.db", since, contains="hi", timeout=600)

CLI (the same functions)::

    http_test_channel.py serve --host H --port P --secret S --db PATH
    http_test_channel.py bind --api-url U --api-key K --agent ID --address A \
        --endpoint URL --adapter NAME
    http_test_channel.py send --api-url U --api-key K --agent ID --address A --adapter NAME --text T
    http_test_channel.py wait --db PATH --since ISO [--contains T] [--conversation C] --timeout S
    http_test_channel.py count --db PATH --since ISO [--until ISO] [--contains T]

Stdlib only. Timestamps are stored as fixed-width UTC ISO strings so they sort
and compare lexicographically.
"""

from __future__ import annotations

import argparse
import json
import sqlite3
import sys
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
import uuid
from datetime import UTC, datetime
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any

ADAPTER_SECRET_HEADER = "X-Curie-Adapter-Secret"
CHANNEL_KIND = "http"
_TS_FORMAT = "%Y-%m-%dT%H:%M:%S.%fZ"
_MAX_BODY = 1024 * 1024

_SCHEMA = """
CREATE TABLE IF NOT EXISTS deliveries (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    received_at TEXT NOT NULL,
    event TEXT,
    conversation_id TEXT,
    text TEXT,
    raw TEXT NOT NULL
)
"""


def utc_now() -> str:
    """The current time in the stored fixed-width UTC format."""
    return datetime.now(UTC).strftime(_TS_FORMAT)


def normalize_ts(value: str) -> str:
    """Parse any ISO-8601 timestamp (naive means UTC) into the stored format."""
    text = value.strip()
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    parsed = datetime.fromisoformat(text)
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC).strftime(_TS_FORMAT)


def _connect(db_path: str) -> sqlite3.Connection:
    conn = sqlite3.connect(db_path, timeout=30)
    conn.row_factory = sqlite3.Row
    conn.execute(_SCHEMA)
    return conn


def _find(obj: Any, key: str) -> Any:
    if isinstance(obj, dict):
        if key in obj:
            return obj[key]
        for value in obj.values():
            found = _find(value, key)
            if found is not None:
                return found
    elif isinstance(obj, list):
        for value in obj:
            found = _find(value, key)
            if found is not None:
                return found
    return None


def extract_text(payload: Any) -> str | None:
    """Reply text: ``message.text`` for reply.post, ``text`` for reply.update."""
    if not isinstance(payload, dict):
        return None
    message = payload.get("message")
    if isinstance(message, dict) and isinstance(message.get("text"), str):
        return message["text"]
    if isinstance(payload.get("text"), str):
        return payload["text"]
    return None


def extract_conversation_id(payload: Any) -> str | None:
    value = _find(payload, "conversation_id")
    return value if isinstance(value, str) else None


class _Handler(BaseHTTPRequestHandler):
    server: _Server

    def log_message(self, format: str, *args: Any) -> None:  # noqa: A002
        return

    def _reply(self, status: int, body: dict[str, Any]) -> None:
        data = json.dumps(body).encode()
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def do_POST(self) -> None:  # noqa: N802
        length = int(self.headers.get("Content-Length") or 0)
        raw = self.rfile.read(min(length, _MAX_BODY)) if length else b""
        if self.headers.get(ADAPTER_SECRET_HEADER) != self.server.secret:
            self._reply(401, {"detail": "unauthorized"})
            return
        try:
            payload = json.loads(raw) if raw else {}
        except ValueError:
            self._reply(400, {"detail": "invalid json"})
            return
        event = payload.get("event") if isinstance(payload, dict) else None
        self.server.store(
            event=event if isinstance(event, str) else None,
            conversation_id=extract_conversation_id(payload),
            text=extract_text(payload),
            raw=raw.decode("utf-8", "replace"),
        )
        self._reply(200, {"ref": f"http-test-{uuid.uuid4().hex}"})


class _Server(ThreadingHTTPServer):
    daemon_threads = True

    def __init__(self, addr: tuple[str, int], secret: str, db_path: str) -> None:
        super().__init__(addr, _Handler)
        self.secret = secret
        self.db_path = db_path
        self._lock = threading.Lock()
        _connect(db_path).close()

    def store(self, **fields: Any) -> None:
        with self._lock:
            conn = _connect(self.db_path)
            try:
                with conn:
                    conn.execute(
                        "INSERT INTO deliveries (received_at, event, conversation_id, text, raw) "
                        "VALUES (?, ?, ?, ?, ?)",
                        (
                            utc_now(),
                            fields["event"],
                            fields["conversation_id"],
                            fields["text"],
                            fields["raw"],
                        ),
                    )
            finally:
                conn.close()


class TestChannelServer:
    """The outbound endpoint. Use as a context manager or call serve_forever()."""

    __test__ = False  # not a pytest class

    def __init__(self, host: str, port: int, secret: str, db_path: str) -> None:
        self._server = _Server((host, port), secret, db_path)
        self._thread: threading.Thread | None = None
        self.db_path = db_path

    @property
    def port(self) -> int:
        return self._server.server_address[1]

    def start(self) -> TestChannelServer:
        self._thread = threading.Thread(target=self._server.serve_forever, daemon=True)
        self._thread.start()
        return self

    def serve_forever(self) -> None:
        self._server.serve_forever()

    def stop(self) -> None:
        self._server.shutdown()
        self._server.server_close()
        if self._thread is not None:
            self._thread.join(timeout=5)

    def __enter__(self) -> TestChannelServer:
        return self.start()

    def __exit__(self, *exc: object) -> None:
        self.stop()


def _http(
    method: str,
    url: str,
    api_key: str,
    body: dict[str, Any] | None = None,
    timeout: float = 30,
) -> tuple[int, Any]:
    data = json.dumps(body).encode() if body is not None else None
    req = urllib.request.Request(url, data=data, method=method)
    req.add_header("X-API-Key", api_key)
    req.add_header("Content-Type", "application/json")
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            status, text = resp.status, resp.read()
    except urllib.error.HTTPError as exc:
        status, text = exc.code, exc.read()
    try:
        parsed = json.loads(text) if text else None
    except ValueError:
        parsed = text.decode("utf-8", "replace")
    return status, parsed


def _check(status: int, body: Any, what: str) -> None:
    if status >= 400:
        raise RuntimeError(f"{what} failed with {status}: {body}")


def bind_channel(
    api_url: str, api_key: str, agent_id: str, address: str, endpoint: str, adapter: str
) -> Any:
    """Create or replace the agent's http binding (route kind/address/adapter)."""
    base = api_url.rstrip("/")
    route = {"kind": CHANNEL_KIND, "address": address, "endpoint": endpoint, "adapter": adapter}
    query = urllib.parse.urlencode({"kind": CHANNEL_KIND, "address": address, "adapter": adapter})
    status, body = _http("PATCH", f"{base}/agents/{agent_id}/channels?{query}", api_key, route)
    if status == 404:
        status, body = _http("POST", f"{base}/agents/{agent_id}/channels", api_key, route)
    _check(status, body, "bind channel")
    return body


def send_turn(
    api_url: str,
    api_key: str,
    agent_id: str,
    address: str,
    adapter: str,
    text: str,
    conversation_id: str | None = None,
    author: str = "http-test-channel",
) -> dict[str, str]:
    """Mint a channel token and enqueue one turn. Returns event_id and conversation_id.

    ``agent_id`` is accepted for call-site symmetry; the binding is resolved by
    the (kind, adapter, address) route, which the token and turn both name.
    """
    base = api_url.rstrip("/")
    status, body = _http(
        "POST",
        f"{base}/channels/token",
        api_key,
        {"kind": CHANNEL_KIND, "address": address, "adapter": adapter},
    )
    _check(status, body, "mint channel token")
    token = body["token"]
    conversation = conversation_id or f"conv-{uuid.uuid4().hex[:12]}"
    status, body = _http(
        "POST",
        f"{base}/channels/turns",
        token,
        {
            "kind": CHANNEL_KIND,
            "address": address,
            "delivery_id": f"http-test-{uuid.uuid4().hex}",
            "conversation_id": conversation,
            "author": author,
            "text": text,
            "reply_ref": f"http-test-ref-{uuid.uuid4().hex[:12]}",
        },
    )
    _check(status, body, "post channel turn")
    return {"event_id": body["event_id"], "conversation_id": conversation}


def _query(
    conn: sqlite3.Connection,
    since: str | None,
    until: str | None,
    contains: str | None,
    conversation: str | None,
) -> list[sqlite3.Row]:
    clauses, args = [], []
    if since:
        clauses.append("received_at >= ?")
        args.append(normalize_ts(since))
    if until:
        clauses.append("received_at < ?")
        args.append(normalize_ts(until))
    if contains:
        clauses.append("instr(COALESCE(text, ''), ?) > 0")
        args.append(contains)
    if conversation:
        clauses.append("conversation_id = ?")
        args.append(conversation)
    where = (" WHERE " + " AND ".join(clauses)) if clauses else ""
    return conn.execute(f"SELECT * FROM deliveries{where} ORDER BY id", args).fetchall()


def _row_dict(row: sqlite3.Row) -> dict[str, Any]:
    out = dict(row)
    out["raw"] = json.loads(out["raw"])
    return out


def count_deliveries(
    db_path: str, since: str | None = None, until: str | None = None, contains: str | None = None
) -> int:
    conn = _connect(db_path)
    try:
        return len(_query(conn, since, until, contains, None))
    finally:
        conn.close()


def wait_for_delivery(
    db_path: str,
    since: str,
    contains: str | None = None,
    conversation: str | None = None,
    timeout: float = 60,
    interval: float = 1.0,
) -> dict[str, Any] | None:
    """Poll until a stored delivery matches; return it, or None on timeout."""
    deadline = time.monotonic() + timeout
    while True:
        conn = _connect(db_path)
        try:
            rows = _query(conn, since, None, contains, conversation)
        finally:
            conn.close()
        if rows:
            return _row_dict(rows[0])
        if time.monotonic() >= deadline:
            return None
        time.sleep(min(interval, max(0.0, deadline - time.monotonic())))


def _main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    sub = parser.add_subparsers(dest="cmd", required=True)

    p = sub.add_parser("serve")
    p.add_argument("--host", required=True)
    p.add_argument("--port", type=int, required=True)
    p.add_argument("--secret", required=True)
    p.add_argument("--db", required=True)

    api = argparse.ArgumentParser(add_help=False)
    api.add_argument("--api-url", required=True)
    api.add_argument("--api-key", required=True)
    api.add_argument("--agent", required=True)
    api.add_argument("--address", required=True)
    api.add_argument("--adapter", required=True)

    p = sub.add_parser("bind", parents=[api])
    p.add_argument("--endpoint", required=True)

    p = sub.add_parser("send", parents=[api])
    p.add_argument("--text", required=True)
    p.add_argument("--conversation")
    p.add_argument("--author", default="http-test-channel")

    p = sub.add_parser("wait")
    p.add_argument("--db", required=True)
    p.add_argument("--since", required=True)
    p.add_argument("--contains")
    p.add_argument("--conversation")
    p.add_argument("--timeout", type=float, required=True)

    p = sub.add_parser("count")
    p.add_argument("--db", required=True)
    p.add_argument("--since", required=True)
    p.add_argument("--until")
    p.add_argument("--contains")

    args = parser.parse_args(argv)
    if args.cmd == "serve":
        server = TestChannelServer(args.host, args.port, args.secret, args.db)
        try:
            server.serve_forever()
        except KeyboardInterrupt:
            pass
        return 0
    if args.cmd == "bind":
        bind_channel(
            args.api_url, args.api_key, args.agent, args.address, args.endpoint, args.adapter
        )
        print(json.dumps({"bound": True, "address": args.address, "adapter": args.adapter}))
        return 0
    if args.cmd == "send":
        result = send_turn(
            args.api_url,
            args.api_key,
            args.agent,
            args.address,
            args.adapter,
            args.text,
            args.conversation,
            args.author,
        )
        print(json.dumps(result))
        return 0
    if args.cmd == "wait":
        hit = wait_for_delivery(args.db, args.since, args.contains, args.conversation, args.timeout)
        if hit is None:
            return 1
        print(json.dumps(hit))
        return 0
    print(count_deliveries(args.db, args.since, args.until, args.contains))
    return 0


if __name__ == "__main__":
    sys.exit(_main())
