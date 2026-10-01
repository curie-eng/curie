"""Scripted external provider: no credentials or real model traffic (#3628)."""

import json
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path


class Handler(BaseHTTPRequestHandler):
    def log_message(self, *_args):
        pass

    def do_GET(self):
        self.send_response(200)
        self.end_headers()
        self.wfile.write(b"ready")

    def do_POST(self):
        data = json.loads(self.rfile.read(int(self.headers["Content-Length"])))
        with Path("/proof/requests.jsonl").open("a") as stream:
            stream.write(json.dumps({"path": self.path, "request": data}) + "\n")
        self.send_response(200)
        if "count_tokens" in self.path:
            self.send_header("Content-Type", "application/json")
            self.end_headers()
            self.wfile.write(b'{"input_tokens":20}')
            return
        self.send_header("Content-Type", "text/event-stream")
        self.end_headers()

        def emit(kind, payload):
            self.wfile.write((f"event: {kind}\ndata: {json.dumps(payload)}\n\n").encode())
            self.wfile.flush()

        # Actual measured provider fields, SDK0.2.159 / CLI2.1.281, issue #3628.
        messages = data["messages"]
        has_result = any(
            block.get("type") == "tool_result"
            for message in messages
            for block in message.get("content", [])
            if isinstance(block, dict)
        )
        capture = "acme capture" in json.dumps(messages[-1]) and not has_result
        emit(
            "message_start",
            {
                "type": "message_start",
                "message": {
                    "id": "msg_acme_capture" if capture else "msg_acme_done",
                    "type": "message",
                    "role": "assistant",
                    "model": data["model"],
                    "content": [],
                    "stop_reason": None,
                    "stop_sequence": None,
                    "usage": {"input_tokens": 20, "output_tokens": 0},
                },
            },
        )
        for index in range(3 if capture else 1):
            if capture:
                # Let the completed first blocks execute before the final fragment.
                if index == 2:
                    deadline = time.monotonic() + 30
                    while not Path("/proof/result-one-observed").exists():
                        if time.monotonic() >= deadline:
                            raise RuntimeError(
                                "native tool result one was not observed before third fragment"
                            )
                        time.sleep(0.05)
                block = {
                    "type": "tool_use",
                    "id": f"call-acme-{index + 1}",
                    "name": "mcp__acme_fixture__read_sample",
                    "input": {},
                }
                delta = {
                    "type": "input_json_delta",
                    "partial_json": json.dumps({"number": index + 1}),
                }
            else:
                block = {"type": "text", "text": ""}
                delta = {"type": "text_delta", "text": "Acme capture complete."}
            emit(
                "content_block_start",
                {"type": "content_block_start", "index": index, "content_block": block},
            )
            emit(
                "content_block_delta",
                {"type": "content_block_delta", "index": index, "delta": delta},
            )
            emit("content_block_stop", {"type": "content_block_stop", "index": index})
            if capture and index == 2:
                Path("/proof/third-fragment-sent").touch()
        emit(
            "message_delta",
            {
                "type": "message_delta",
                "delta": {
                    "stop_reason": "tool_use" if capture else "end_turn",
                    "stop_sequence": None,
                },
                "usage": {"output_tokens": 20},
            },
        )
        emit("message_stop", {"type": "message_stop"})


if __name__ == "__main__":
    ThreadingHTTPServer(("0.0.0.0", 18579), Handler).serve_forever()
