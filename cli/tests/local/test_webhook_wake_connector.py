"""Exercise the hosted qualification fixture through its HTTP and MCP ports."""

from __future__ import annotations

import asyncio
import hashlib
import hmac
import json
import socket
import threading
import urllib.request
from contextlib import contextmanager
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import httpx2
import pytest
import uvicorn
from mcp import ClientSession
from mcp.client.streamable_http import streamable_http_client
from webhook_wake_connector import Queue, build_mcp, control_server

KEY = "example-connector-wake-key"
AGENT = "00000000-0000-4000-8000-000000000001"


@contextmanager
def serving(server):
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield f"http://127.0.0.1:{server.server_port}"
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=10)
        if thread.is_alive():
            raise RuntimeError("fixture HTTP thread did not stop")


def post(url, value=None):
    with urllib.request.urlopen(
        urllib.request.Request(
            url,
            data=json.dumps(value or {}).encode(),
            headers={"Content-Type": "application/json"},
        ),
        timeout=10,
    ) as response:
        return response.status, json.loads(response.read())


def test_empty_poll_never_sends_a_wake_and_items_use_fresh_signed_deliveries(tmp_path):
    received = []

    class Receiver(BaseHTTPRequestHandler):
        def do_POST(self):
            body = self.rfile.read(int(self.headers["Content-Length"]))
            received.append((self.path, dict(self.headers), body))
            answer = b'{"status":"coalesced"}'
            self.send_response(200)
            self.send_header("Content-Length", str(len(answer)))
            self.end_headers()
            self.wfile.write(answer)

        def log_message(self, *_args):
            return

    queue = Queue(tmp_path / "queue.sqlite3")
    with serving(ThreadingHTTPServer(("127.0.0.1", 0), Receiver)) as wake_api:
        with serving(
            control_server(
                queue, f"{wake_api}/hooks/{AGENT}/queue-ready/wake", KEY, ("127.0.0.1", 0)
            )
        ) as control:
            assert post(control + "/poll") == (200, {"status": "empty"})
            assert received == []
            assert post(control + "/items", {"id": "item-1", "text": "record receipt"})[0] == 201
            assert post(control + "/poll")[1]["http_status"] == 200
            assert post(control + "/poll")[1]["http_status"] == 200
    assert len(received) == 2
    deliveries = set()
    for path, headers, body in received:
        assert path == f"/hooks/{AGENT}/queue-ready/wake" and body == b"{}"
        timestamp = headers["X-Curie-Timestamp"]
        delivery = headers["X-Curie-Delivery-Id"]
        deliveries.add(delivery)
        context = json.dumps([AGENT, "queue-ready"], separators=(",", ":")).encode()
        material = (
            b"curie.hook.wake.v1\n"
            + f"{timestamp}.{delivery}.{len(context)}:".encode()
            + context
            + body
        )
        expected = "sha256=" + hmac.new(KEY.encode(), material, hashlib.sha256).hexdigest()
        assert hmac.compare_digest(headers["X-Curie-Signature-256"], expected)
    assert len(deliveries) == 2


@pytest.fixture
def mcp_queue(tmp_path):
    queue = Queue(tmp_path / "queue.sqlite3")
    ready = threading.Event()

    class Server(uvicorn.Server):
        async def startup(self, sockets=None):
            await super().startup(sockets=sockets)
            ready.set()

    with socket.socket() as listener:
        listener.bind(("127.0.0.1", 0))
        server = Server(
            uvicorn.Config(
                build_mcp(queue, KEY).streamable_http_app(), log_level="error", access_log=False
            )
        )
        thread = threading.Thread(target=server.run, kwargs={"sockets": [listener]}, daemon=True)
        thread.start()
        try:
            if not ready.wait(timeout=10) or not server.started:
                raise RuntimeError("fixture MCP server did not start")
            yield queue, f"http://127.0.0.1:{listener.getsockname()[1]}/mcp"
        finally:
            server.should_exit = True
            thread.join(timeout=10)
            if thread.is_alive():
                raise RuntimeError("fixture MCP server did not stop")


def test_real_mcp_reads_and_records_once_without_exposing_the_key(mcp_queue):
    queue, url = mcp_queue
    queue.add("item-1", "record receipt")

    async def exercise():
        async with streamable_http_client(url) as (read, write):
            async with ClientSession(read, write) as session:
                await session.initialize()
                names = {item.name for item in (await session.list_tools()).tools}
                assert names == {"queue_read", "queue_complete"}
                read_result = await session.call_tool("queue_read", {})
                assert not read_result.is_error
                assert KEY not in read_result.model_dump_json()
                assert json.loads(read_result.content[0].text) == {
                    "items": [{"id": "item-1", "text": "record receipt"}]
                }
                result = await session.call_tool(
                    "queue_complete", {"item_id": "item-1", "result": "receipt-ok"}
                )
                assert not result.is_error and KEY not in result.model_dump_json()
                duplicate = await session.call_tool(
                    "queue_complete", {"item_id": "item-1", "result": "overwrite"}
                )
                assert duplicate.is_error

    asyncio.run(exercise())
    observation = queue.observation()
    assert observation["items"] == [{"id": "item-1", "result": "receipt-ok"}]
    assert all(not call["human_header"] and not call["key_header"] for call in observation["calls"])


def test_mcp_key_leak_is_refused_before_the_item_is_changed(mcp_queue):
    queue, url = mcp_queue
    queue.add("item-1", "record receipt")

    async def exercise():
        async with httpx2.AsyncClient(headers={"X-Example-Credential": KEY}) as client:
            async with streamable_http_client(url, http_client=client) as (read, write):
                async with ClientSession(read, write) as session:
                    await session.initialize()
                    result = await session.call_tool(
                        "queue_complete", {"item_id": "item-1", "result": "must-not-write"}
                    )
                    assert result.is_error
                    assert KEY not in result.model_dump_json()

    asyncio.run(exercise())
    assert queue.observation()["items"] == [{"id": "item-1", "result": None}]
    assert queue.observation()["calls"] == [
        {"tool": "complete", "human_header": False, "key_header": True, "agent": ""}
    ]


def test_read_and_completion_barriers_keep_a_late_item_for_the_next_poll(mcp_queue):
    queue, url = mcp_queue
    queue.add("item-1", "first item")
    queue.barriers(read=True, complete=True)

    async def exercise():
        async with streamable_http_client(url) as (read, write):
            async with ClientSession(read, write) as session:
                await session.initialize()
                reading = asyncio.create_task(session.call_tool("queue_read", {}))
                try:
                    assert await asyncio.to_thread(queue.read_seen.wait, 5)
                    queue.add("item-2", "arrived after the read")
                    queue.barriers(read=False, complete=True)
                    result = await reading
                    assert json.loads(result.content[0].text) == {
                        "items": [{"id": "item-1", "text": "first item"}]
                    }
                    completing = asyncio.create_task(
                        session.call_tool(
                            "queue_complete", {"item_id": "item-1", "result": "receipt-ok"}
                        )
                    )
                    assert await asyncio.to_thread(queue.complete_seen.wait, 5)
                    assert queue.observation()["items"] == [
                        {"id": "item-1", "result": "receipt-ok"},
                        {"id": "item-2", "result": None},
                    ]
                    queue.barriers()
                    assert not (await completing).is_error
                finally:
                    queue.barriers()
                    await reading

    asyncio.run(exercise())
    assert queue.pending() == [{"id": "item-2", "text": "arrived after the read"}]
