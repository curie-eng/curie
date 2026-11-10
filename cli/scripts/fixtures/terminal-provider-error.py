#!/usr/bin/env python3
"""A private, credential-free provider failure for the actual SDK ladder.

Retry directive grounding: https://github.com/anthropics/anthropic-sdk-typescript/blob/main/src/client.ts
Observed with claude-agent-sdk0.2.159: this directive produces one HTTP request
and a terminal model-credential-rejected event instead of transport retries.
"""

from __future__ import annotations

import argparse
import json
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from threading import Lock


class Provider(ThreadingHTTPServer):
    requests = 0
    counter_lock = Lock()


class Handler(BaseHTTPRequestHandler):
    server: Provider

    def log_message(self, format: str, *args: object) -> None:
        # Never log request paths, headers, bodies, or error details.
        pass

    def respond(self, status: int, payload: dict[str, object]) -> None:
        body = json.dumps(payload).encode()
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        if status == 401:
            self.send_header("x-should-retry", "false")
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self) -> None:
        if self.path != "/health":
            self.respond(404, {"error": "not found"})
            return
        with self.server.counter_lock:
            count = self.server.requests
        self.respond(200, {"requests": count})

    def do_POST(self) -> None:
        with self.server.counter_lock:
            self.server.requests += 1
        # Drain payload bytes without retaining or logging them; closing with
        # an unread request can cause a TCP reset instead of the intended401.
        remaining = int(self.headers.get("Content-Length", "0"))
        while remaining > 0:
            chunk = self.rfile.read(min(remaining, 65536))
            if not chunk:
                break
            remaining -= len(chunk)
        self.respond(
            401,
            {
                "type": "error",
                "error": {
                    "type": "authentication_error",
                    "message": "synthetic credential rejected",
                },
            },
        )


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=0)
    args = parser.parse_args()
    server = Provider((args.host, args.port), Handler)
    print(json.dumps({"port": server.server_port}), flush=True)
    server.serve_forever()


if __name__ == "__main__":
    main()
