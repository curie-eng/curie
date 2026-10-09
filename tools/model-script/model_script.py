"""Scripted Anthropic Messages endpoint for factory runs (#3814).

``serve`` replays a transcript. Requests are matched by the scrubbed first
user message and the number of assistant messages in that lane. An unexpected
request is answered as an error and fails the process. ``record`` proxies each
request to an Anthropic-compatible provider and appends its replay fields.

The Claude Agent SDK posts ``/v1/messages`` on ``CURIE_MODEL_BASE_URL`` and
usually sets ``stream: true``. Streaming frames follow the Messages streaming
events documented at https://platform.claude.com/docs/en/api/messages-streaming.
OpenRouter's Anthropic-compatible base is ``https://openrouter.ai/api``
(https://openrouter.ai/docs/api-reference/overview).
"""

from __future__ import annotations

import argparse
import json
import os
import re
import signal
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

_VOLATILE = re.compile(
    r"https?://[^\s\"'\\<>]+"
    r"|toolu_[A-Za-z0-9]+"
    r"|msg_[A-Za-z0-9]+"
    r"|[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}"
    r"|\b[0-9a-f]{40}\b"
    r"|\b\d{4}-\d{2}-\d{2}(?:T\d{2}:\d{2}:\d{2}(?:\.\d+)?(?:Z|[+-]\d{2}:\d{2})?)?"
    r"|\b\d+\.\d+\s*(?:seconds?|s)\b"
)


class UnexpectedRequest(RuntimeError):
    """A served request was not in the transcript."""


def scrub(text: str) -> str:
    """Replace volatile tokens so two runs of the same turn still match."""

    return _VOLATILE.sub("<v>", text)


def request_key(request: dict[str, Any]) -> tuple[str, int] | None:
    """Identify a conversation lane and its next assistant response."""

    messages = request.get("messages")
    if not isinstance(messages, list):
        return None
    first_user = next(
        (
            message
            for message in messages
            if isinstance(message, dict) and message.get("role") == "user"
        ),
        None,
    )
    if first_user is None:
        return None
    content = first_user.get("content")
    if isinstance(content, str):
        text = content
    elif isinstance(content, list):
        text = "\n".join(
            block["text"]
            for block in content
            if isinstance(block, dict)
            and block.get("type") == "text"
            and isinstance(block.get("text"), str)
        )
    else:
        return None
    ordinal = sum(
        isinstance(message, dict) and message.get("role") == "assistant"
        for message in messages
    )
    return scrub(text), ordinal


def message_to_sse(message: dict[str, Any]) -> bytes:
    """Encode one Messages object as the SSE stream the SDK reads."""

    frames: list[str] = []

    def emit(event: str, data: dict[str, Any]) -> None:
        frames.append(f"event: {event}\ndata: {json.dumps(data, separators=(',', ':'))}\n\n")

    usage = message.get("usage")
    if not isinstance(usage, dict):
        usage = {}
    start: dict[str, Any] = {
        "id": message.get("id") or "msg_scripted",
        "type": "message",
        "role": "assistant",
        "model": message.get("model") or "scripted",
        "content": [],
        "stop_reason": None,
        "stop_sequence": None,
        "usage": {
            "input_tokens": usage.get("input_tokens", 1),
            "output_tokens": 0,
        },
    }
    emit("message_start", {"type": "message_start", "message": start})
    for index, block in enumerate(message.get("content") or []):
        if not isinstance(block, dict):
            continue
        kind = block.get("type")
        if kind == "tool_use":
            emit(
                "content_block_start",
                {
                    "type": "content_block_start",
                    "index": index,
                    "content_block": {
                        "type": "tool_use",
                        "id": block.get("id") or "toolu_scripted",
                        "name": block.get("name") or "tool",
                        "input": {},
                    },
                },
            )
            emit(
                "content_block_delta",
                {
                    "type": "content_block_delta",
                    "index": index,
                    "delta": {
                        "type": "input_json_delta",
                        "partial_json": json.dumps(block.get("input") or {}, separators=(",", ":")),
                    },
                },
            )
        else:
            emit(
                "content_block_start",
                {
                    "type": "content_block_start",
                    "index": index,
                    "content_block": {"type": "text", "text": ""},
                },
            )
            emit(
                "content_block_delta",
                {
                    "type": "content_block_delta",
                    "index": index,
                    "delta": {"type": "text_delta", "text": str(block.get("text") or "")},
                },
            )
        emit("content_block_stop", {"type": "content_block_stop", "index": index})
    emit(
        "message_delta",
        {
            "type": "message_delta",
            "delta": {
                "stop_reason": message.get("stop_reason") or "end_turn",
                "stop_sequence": None,
            },
            "usage": {"output_tokens": usage.get("output_tokens", 1)},
        },
    )
    emit("message_stop", {"type": "message_stop"})
    return "".join(frames).encode()


def sse_to_message(body: bytes) -> dict[str, Any] | None:
    """Assemble a Messages object from an SSE body, or None when it is not one."""

    text = body.decode("utf-8", errors="replace")
    if "event:" not in text:
        return None
    message: dict[str, Any] | None = None
    blocks: list[dict[str, Any]] = []
    partials: dict[int, list[str]] = {}
    stop_reason = "end_turn"
    for chunk in text.split("\n\n"):
        event = ""
        data = ""
        for line in chunk.splitlines():
            if line.startswith("event:"):
                event = line.split(":", 1)[1].strip()
            elif line.startswith("data:"):
                data += line.split(":", 1)[1].strip()
        if not data or data == "[DONE]":
            continue
        try:
            payload = json.loads(data)
        except json.JSONDecodeError:
            return None
        if event == "message_start" and isinstance(payload.get("message"), dict):
            message = dict(payload["message"])
            message["content"] = []
        elif event == "content_block_start":
            block = payload.get("content_block")
            if isinstance(block, dict):
                blocks.append(dict(block))
                partials[int(payload.get("index") or len(blocks) - 1)] = []
        elif event == "content_block_delta":
            index = int(payload.get("index") or 0)
            delta = payload.get("delta") if isinstance(payload.get("delta"), dict) else {}
            if delta.get("type") == "text_delta":
                partials.setdefault(index, []).append(str(delta.get("text") or ""))
            elif delta.get("type") == "input_json_delta":
                partials.setdefault(index, []).append(str(delta.get("partial_json") or ""))
        elif event == "message_delta":
            delta = payload.get("delta") if isinstance(payload.get("delta"), dict) else {}
            if delta.get("stop_reason"):
                stop_reason = str(delta["stop_reason"])
    if message is None:
        return None
    content: list[dict[str, Any]] = []
    for index, block in enumerate(blocks):
        joined = "".join(partials.get(index, []))
        if block.get("type") == "tool_use":
            try:
                tool_input = json.loads(joined) if joined else {}
            except json.JSONDecodeError:
                tool_input = {}
            content.append(
                {
                    "type": "tool_use",
                    "id": block.get("id"),
                    "name": block.get("name"),
                    "input": tool_input,
                }
            )
        else:
            content.append({"type": "text", "text": joined or str(block.get("text") or "")})
    message["content"] = content
    message["stop_reason"] = stop_reason
    return message


def load_transcript(path: Path) -> list[dict[str, Any]]:
    raw = json.loads(path.read_text())
    exchanges = raw.get("exchanges") if isinstance(raw, dict) else None
    if not isinstance(exchanges, list):
        raise ValueError("transcript exchanges must be a list")
    for exchange in exchanges:
        if (
            not isinstance(exchange, dict)
            or set(exchange) != {"lane_key", "ordinal", "message"}
            or not isinstance(exchange["lane_key"], str)
            or type(exchange["ordinal"]) is not int
            or exchange["ordinal"] < 0
            or not isinstance(exchange["message"], dict)
        ):
            raise ValueError("transcript exchange requires lane_key, ordinal, and message")
    return exchanges


def dump_transcript(path: Path, exchanges: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps({"version": 1, "exchanges": exchanges}, indent=2) + "\n"
    )


class ModelScript:
    """One loopback or pod-facing Messages server."""

    def __init__(
        self,
        transcript: Path,
        *,
        record: bool = False,
        upstream: str = "https://openrouter.ai/api",
        host: str = "127.0.0.1",
        port: int = 0,
    ) -> None:
        self.transcript = transcript
        self.record = record
        self.upstream = upstream.rstrip("/")
        self.host = host
        self._record_key = os.environ.get("CURIE_FACTORY_RECORD_KEY", "") if record else ""
        if record and not self._record_key:
            raise ValueError("recording requires CURIE_FACTORY_RECORD_KEY")
        self._exchanges = [] if record else load_transcript(transcript)
        self._used: set[int] = set()
        self.unexpected: list[dict[str, Any]] = []
        self._lock = threading.Lock()
        script = self

        class Handler(BaseHTTPRequestHandler):
            protocol_version = "HTTP/1.1"

            def log_message(self, fmt: str, *args: Any) -> None:
                return

            def do_GET(self) -> None:  # noqa: N802
                if self.path.split("?", 1)[0] in {"/", "/health"}:
                    body = b'{"ok":true}'
                    self.send_response(200)
                    self.send_header("Content-Type", "application/json")
                    self.send_header("Content-Length", str(len(body)))
                    self.end_headers()
                    self.wfile.write(body)
                    return
                script._reject(self, "GET", self.path, b"")

            def do_POST(self) -> None:  # noqa: N802
                length = int(self.headers.get("Content-Length", "0"))
                body = self.rfile.read(length)
                path = self.path.split("?", 1)[0]
                if path != "/v1/messages":
                    script._reject(self, "POST", path, body)
                    return
                if script.record:
                    script._record(self, path, body)
                    return
                script._serve(self, path, body)

        self._server = ThreadingHTTPServer((host, port), Handler)
        self._thread = threading.Thread(target=self._server.serve_forever, daemon=True)
        self._closed = False

    @property
    def port(self) -> int:
        return int(self._server.server_address[1])

    @property
    def base_url(self) -> str:
        host = self.host if self.host != "0.0.0.0" else "127.0.0.1"
        return f"http://{host}:{self.port}"

    def start(self) -> None:
        self._thread.start()

    def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        if self._thread.is_alive():
            self._server.shutdown()
            self._thread.join(timeout=5)
        self._server.server_close()
        if self.record:
            dump_transcript(self.transcript, self._exchanges)
        if self.unexpected:
            raise UnexpectedRequest(
                f"unexpected model request: {len(self.unexpected)} request(s) "
                "were not in the transcript"
            )

    def _match(self, method: str, path: str, body: bytes) -> dict[str, Any] | None:
        if method != "POST" or path != "/v1/messages":
            return None
        parsed = _safe_json(body)
        wanted = request_key(parsed)
        if wanted is None:
            return None
        for index, exchange in enumerate(self._exchanges):
            if index in self._used:
                continue
            if (exchange["lane_key"], exchange["ordinal"]) == wanted:
                self._used.add(index)
                return exchange
        return None

    def _serve(self, handler: BaseHTTPRequestHandler, path: str, body: bytes) -> None:
        with self._lock:
            exchange = self._match("POST", path, body)
            if exchange is None:
                self.unexpected.append(
                    {
                        "method": "POST",
                        "path": path,
                        "key": request_key(_safe_json(body)),
                    }
                )
        if exchange is None:
            payload = json.dumps(
                {
                    "type": "error",
                    "error": {
                        "type": "unexpected_request",
                        "message": "unexpected model request: the transcript has no unused "
                        "exchange for this lane and ordinal",
                    },
                }
            ).encode()
            handler.send_response(422)
            handler.send_header("Content-Type", "application/json")
            handler.send_header("Content-Length", str(len(payload)))
            handler.end_headers()
            handler.wfile.write(payload)
            return
        request = _safe_json(body)
        wants_stream = bool(request.get("stream"))
        if wants_stream:
            payload = message_to_sse(exchange["message"])
            content_type = "text/event-stream"
        else:
            payload = json.dumps(exchange["message"]).encode()
            content_type = "application/json"
        handler.send_response(200)
        handler.send_header("Content-Type", content_type)
        handler.send_header("Content-Length", str(len(payload)))
        handler.end_headers()
        handler.wfile.write(payload)

    def _record(self, handler: BaseHTTPRequestHandler, path: str, body: bytes) -> None:
        key = request_key(_safe_json(body))
        if key is None:
            self._reject(handler, "POST", path, body)
            return
        headers = {
            "Content-Type": handler.headers.get("Content-Type", "application/json"),
            "x-api-key": self._record_key,
        }
        for name in ("anthropic-version", "anthropic-beta"):
            value = handler.headers.get(name)
            if value:
                headers[name] = value
        request = Request(
            self.upstream + path,
            data=body,
            headers=headers,
            method="POST",
        )
        try:
            with urlopen(request, timeout=600) as response:
                status = response.status
                content_type = response.headers.get("Content-Type", "application/json")
                payload = response.read()
        except HTTPError as exc:
            status = exc.code
            header = exc.headers.get("Content-Type") if exc.headers else None
            content_type = header or "application/json"
            payload = exc.read()
        except URLError as exc:
            payload = json.dumps({"type": "error", "error": {"message": str(exc.reason)}}).encode()
            status = 502
            content_type = "application/json"
        if status < 400:
            message = None
            if "event-stream" in content_type:
                message = sse_to_message(payload)
            else:
                try:
                    parsed = json.loads(payload)
                except json.JSONDecodeError:
                    parsed = None
                if isinstance(parsed, dict) and parsed.get("type") == "message":
                    message = parsed
            if message is None:
                status = 502
                content_type = "application/json"
                payload = (
                    b'{"type":"error","error":{"message":'
                    b'"provider returned no Messages response"}}'
                )
            else:
                with self._lock:
                    self._exchanges.append(
                        {"lane_key": key[0], "ordinal": key[1], "message": message}
                    )
        handler.send_response(status)
        handler.send_header("Content-Type", content_type)
        handler.send_header("Content-Length", str(len(payload)))
        handler.end_headers()
        handler.wfile.write(payload)

    def _reject(self, handler: BaseHTTPRequestHandler, method: str, path: str, body: bytes) -> None:
        with self._lock:
            self.unexpected.append({"method": method, "path": path.split("?", 1)[0]})
        payload = b'{"type":"error","error":{"type":"unexpected_request"}}'
        handler.send_response(422)
        handler.send_header("Content-Type", "application/json")
        handler.send_header("Content-Length", str(len(payload)))
        handler.end_headers()
        handler.wfile.write(payload)


def _safe_json(body: bytes) -> dict[str, Any]:
    try:
        parsed = json.loads(body) if body else {}
    except (json.JSONDecodeError, UnicodeDecodeError):
        return {}
    return parsed if isinstance(parsed, dict) else {}


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    serve = commands.add_parser("serve", help="Replay a transcript")
    serve.add_argument("--transcript", type=Path, required=True)
    serve.add_argument("--host", default="127.0.0.1")
    serve.add_argument("--port", type=int, default=0)
    record = commands.add_parser("record", help="Proxy to a provider and write a transcript")
    record.add_argument("--output", type=Path, required=True)
    record.add_argument("--upstream", default="https://openrouter.ai/api")
    record.add_argument("--host", default="127.0.0.1")
    record.add_argument("--port", type=int, default=0)
    args = parser.parse_args(argv)
    recording = args.command == "record"
    try:
        script = ModelScript(
            args.output if recording else args.transcript,
            record=recording,
            upstream=getattr(args, "upstream", "https://openrouter.ai/api"),
            host=args.host,
            port=args.port,
        )
    except (OSError, ValueError) as exc:
        parser.exit(1, f"model-script: {exc}\n")
    stop = threading.Event()

    def _stop(_signum: int, _frame: Any) -> None:
        stop.set()

    signal.signal(signal.SIGINT, _stop)
    signal.signal(signal.SIGTERM, _stop)
    try:
        script.start()
        print(
            json.dumps(
                {
                    "base_url": script.base_url,
                    "mode": args.command,
                    "transcript": str(args.output if recording else args.transcript),
                }
            ),
            flush=True,
        )
        while not stop.wait(0.2) and not script.unexpected:
            pass
        script.close()
    except UnexpectedRequest as exc:
        print(f"model-script: {exc}", flush=True)
        return 1
    except (OSError, ValueError) as exc:
        parser.exit(1, f"model-script: {exc}\n")
    return 1 if script.unexpected else 0


if __name__ == "__main__":
    raise SystemExit(main())
