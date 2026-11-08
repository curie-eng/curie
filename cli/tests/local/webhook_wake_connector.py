"""Hosted queue fixture for ADR 0274, with a connector-only wake credential.

MCP reads and writes use the normal hosted connector path. The control port
stages external work and performs one poll. It never returns the wake key.
"""

import hashlib
import hmac
import json
import os
import sqlite3
import threading
import time
import uuid
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

from mcp.server.mcpserver import Context, MCPServer
from mcp.server.mcpserver.exceptions import ToolError
from mcp.types import ToolAnnotations


class Queue:
    def __init__(self, path):
        self.path = path
        with self.connect() as db:
            db.execute(
                "CREATE TABLE IF NOT EXISTS items "
                "(id TEXT PRIMARY KEY, text TEXT NOT NULL, result TEXT)"
            )
            db.execute(
                "CREATE TABLE IF NOT EXISTS calls "
                "(tool TEXT NOT NULL, human_header INTEGER NOT NULL, key_header INTEGER NOT NULL)"
            )
            db.execute("CREATE TABLE IF NOT EXISTS polls (delivery TEXT, status INTEGER)")

    def connect(self):
        return sqlite3.connect(self.path, timeout=10)

    def add(self, item, text):
        with self.connect() as db:
            db.execute("INSERT INTO items(id,text) VALUES(?,?)", (item, text))

    def pending(self):
        with self.connect() as db:
            return [
                {"id": row[0], "text": row[1]}
                for row in db.execute("SELECT id,text FROM items WHERE result IS NULL ORDER BY id")
            ]

    def complete(self, item, result):
        with self.connect() as db:
            cursor = db.execute(
                "UPDATE items SET result=? WHERE id=? AND result IS NULL", (result, item)
            )
            if cursor.rowcount != 1:
                raise ToolError("item_not_pending")
        return {"recorded": True, "id": item}

    def audit(self, tool, headers, key):
        human = any(
            name.lower() in {"x-curie-user", "x-curie-user-email", "x-curie-principal"}
            for name in headers
        )
        leaked = any(key in value for value in headers.values())
        with self.connect() as db:
            db.execute("INSERT INTO calls VALUES(?,?,?)", (tool, human, leaked))
        if leaked:
            raise ToolError("wake_credential_in_mcp_headers")

    def observation(self):
        with self.connect() as db:
            return {
                "items": [
                    {"id": row[0], "result": row[1]}
                    for row in db.execute("SELECT id,result FROM items ORDER BY id")
                ],
                "calls": [
                    {"tool": row[0], "human_header": bool(row[1]), "key_header": bool(row[2])}
                    for row in db.execute("SELECT * FROM calls")
                ],
                "polls": [row[0] for row in db.execute("SELECT status FROM polls")],
            }

    def poll(self, url, key):
        if not self.pending():
            return {"status": "empty"}
        import urllib.parse

        path = urllib.parse.urlsplit(url).path.split("/")
        if len(path) != 5 or path[1] != "hooks" or path[4] != "wake":
            raise ValueError("invalid wake URL")
        context = json.dumps([path[2], path[3]], separators=(",", ":")).encode()
        delivery = uuid.uuid4().hex
        timestamp = str(int(time.time()))
        material = (
            b"curie.hook.wake.v1\n"
            + f"{timestamp}.{delivery}.{len(context)}:".encode()
            + context
            + b"{}"
        )
        request = urllib.request.Request(
            url,
            data=b"{}",
            headers={
                "Content-Type": "application/json",
                "X-Curie-Timestamp": timestamp,
                "X-Curie-Delivery-Id": delivery,
                "X-Curie-Signature-256": "sha256="
                + hmac.new(key.encode(), material, hashlib.sha256).hexdigest(),
            },
        )
        try:
            response = urllib.request.urlopen(request, timeout=30)
        except urllib.error.HTTPError as error:
            response = error
        with response:
            payload = json.loads(response.read())
            status = response.status
        with self.connect() as db:
            db.execute("INSERT INTO polls VALUES(?,?)", (delivery, status))
        return {"http_status": status, "receipt": payload}


def control_server(queue, url, key, address=("0.0.0.0", 8001)):
    class Control(BaseHTTPRequestHandler):
        def log_message(self, *_args):
            return

        def answer(self, status, value):
            body = json.dumps(value).encode()
            self.send_response(status)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def do_GET(self):
            if self.path == "/observation":
                self.answer(200, queue.observation())
            else:
                self.answer(404, {"error": "unknown_path"})

        def do_POST(self):
            length = int(self.headers.get("Content-Length", "0"))
            if not 0 <= length <= 4096:
                self.answer(413, {"error": "body_too_large"})
                return
            try:
                value = json.loads(self.rfile.read(length) or b"{}")
                if self.path == "/items":
                    queue.add(value["id"], value["text"])
                    self.answer(201, {"stored": True})
                elif self.path == "/poll":
                    self.answer(200, queue.poll(url, key))
                else:
                    self.answer(404, {"error": "unknown_path"})
            except (ValueError, KeyError, TypeError, sqlite3.IntegrityError):
                self.answer(400, {"error": "invalid_item"})
            except OSError:
                self.answer(503, {"error": "wake_unavailable"})

    return ThreadingHTTPServer(address, Control)


def build_mcp(queue, key):
    server = MCPServer("wake-queue")

    @server.tool(annotations=ToolAnnotations(read_only_hint=True, open_world_hint=False))
    def queue_read(ctx: Context) -> str:
        """Read pending queue items. Use queue_complete to record each result."""
        queue.audit("read", ctx.headers or {}, key)
        return json.dumps({"items": queue.pending()})

    @server.tool(annotations=ToolAnnotations(read_only_hint=False, open_world_hint=False))
    def queue_complete(item_id: str, result: str, ctx: Context) -> str:
        """Record the result for one pending item exactly once."""
        queue.audit("complete", ctx.headers or {}, key)
        return json.dumps(queue.complete(item_id, result))

    return server


def main():
    key = os.environ["CURIE_HOOK_WAKE_KEY"]
    url = os.environ["WAKE_URL"]
    queue = Queue(Path(os.environ.get("WAKE_QUEUE_DB", "/tmp/wake-queue.sqlite3")))
    control = control_server(queue, url, key)
    thread = threading.Thread(target=control.serve_forever, daemon=True)
    thread.start()
    try:
        build_mcp(queue, key).run(
            transport="streamable-http", host="0.0.0.0", port=8000, streamable_http_path="/mcp"
        )
    finally:
        control.shutdown()
        control.server_close()
        thread.join()


if __name__ == "__main__":
    main()
