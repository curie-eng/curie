"""Stub upstream API for the `ui-image-smoke` CI job.

The job puts this server behind the console image's `/api/` proxy location so
the smoke checks can prove that proxied API responses stay uncompressed,
byte-identical to what the upstream wrote, and incrementally streamed rather
than buffered. Two behaviors are served: a large JSON body with an explicit
Content-Length, well over `gzip_min_length` so gzip would certainly fire if the
`gzip off` in that location were ever deleted, and an SSE stream emitted as
timed chunked writes so buffering can be measured rather than assumed.

CI only. This file is never copied into the shipped UI image.
"""

import json
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

PAYLOAD = json.dumps(
    {"agents": [{"id": i, "name": f"agent-{i}", "status": "ready"} for i in range(2000)]}
).encode()


class Handler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def log_message(self, fmt, *args):
        return

    def do_GET(self):
        if self.path.startswith("/stream"):
            self.send_response(200)
            self.send_header("Content-Type", "text/event-stream")
            self.send_header("Cache-Control", "no-cache")
            self.send_header("Transfer-Encoding", "chunked")
            self.end_headers()
            for n in range(5):
                body = f"data: tick {n}\n\n".encode()
                self.wfile.write(f"{len(body):x}\r\n".encode() + body + b"\r\n")
                self.wfile.flush()
                time.sleep(0.4)
            self.wfile.write(b"0\r\n\r\n")
            self.wfile.flush()
            return
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(PAYLOAD)))
        self.end_headers()
        self.wfile.write(PAYLOAD)
        self.wfile.flush()


ThreadingHTTPServer(("0.0.0.0", 8000), Handler).serve_forever()
