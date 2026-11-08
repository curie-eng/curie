"""A disposable RESP transport fault, outside the installed application's boundary."""

import json
import os
import socket
import socketserver
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer


def frame(stream):
    line = stream.readline()
    if not line:
        raise EOFError
    prefix, value = line[:1], line[1:-2]
    if prefix in (b"$", b"!", b"="):
        length = int(value)
        body = stream.read(length + 2) if length >= 0 else b""
        return line + body, body[:-2] if length >= 0 else None
    if prefix in (b"*", b"%", b"~", b"|", b">"):
        count = max(0, int(value)) * (2 if prefix in (b"%", b"|") else 1)
        parts = [frame(stream) for _ in range(count)]
        raw = line + b"".join(raw for raw, _ in parts)
        if prefix == b"|":
            following, result = frame(stream)
            return raw + following, result
        return raw, [value for _, value in parts]
    if prefix in (b"+", b"-", b":", b",", b"#", b"_", b"("):
        return line, value
    raise ValueError("unexpected RESP frame")


class Fault:
    mode = "forward"
    dropped = 0
    refused = 0
    forwarded = 0


def main():
    state = Fault()

    class Proxy(socketserver.StreamRequestHandler):
        def handle(self):
            with socket.create_connection((os.environ["UPSTREAM_HOST"], 6379), timeout=5) as peer:
                with peer.makefile("rb") as upstream:
                    try:
                        while True:
                            raw, command = frame(self.rfile)
                            if state.mode == "down":
                                state.refused += 1
                                return
                            if state.mode == "drop_xadd" and command[0].upper() == b"XADD":
                                state.dropped += 1
                                return
                            peer.sendall(raw)
                            state.forwarded += 1
                            reply, _ = frame(upstream)
                            self.wfile.write(reply)
                            self.wfile.flush()
                    except (EOFError, OSError):
                        return

    class Control(BaseHTTPRequestHandler):
        def log_message(self, *_args):
            return

        def do_GET(self):
            raw = json.dumps(
                {
                    "mode": state.mode,
                    "dropped": state.dropped,
                    "refused": state.refused,
                    "forwarded": state.forwarded,
                }
            ).encode()
            self.send_response(200)
            self.send_header("Content-Length", str(len(raw)))
            self.end_headers()
            self.wfile.write(raw)

        def do_POST(self):
            value = json.loads(self.rfile.read(int(self.headers["Content-Length"])))
            if value.get("mode") not in {"forward", "down", "drop_xadd"}:
                self.send_error(400)
                return
            state.mode = value["mode"]
            self.do_GET()

    class Server(socketserver.ThreadingTCPServer):
        allow_reuse_address = True
        daemon_threads = True

    with Server(("0.0.0.0", 6379), Proxy) as redis:
        thread = threading.Thread(target=redis.serve_forever, daemon=True)
        thread.start()
        try:
            ThreadingHTTPServer(("0.0.0.0", 8000), Control).serve_forever()
        finally:
            redis.shutdown()
            thread.join(timeout=10)


if __name__ == "__main__":
    main()
