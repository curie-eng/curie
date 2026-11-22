"""A forge proxy that can fail chosen calls on purpose (#5429).

The publication recovery proof needs one controlled transport failure between
Curie and its forge after the publication Job has pushed: the API's pull
request create must fail, the same way it would against a forge outage, while
every other call reaches the forge unchanged. This program stands in for the
whole forge: the install's GitHub API base, clone base, tracker and code host
entries all name it, and it forwards every request (REST calls and git smart
HTTP alike) to the real forge origin.

Curie checks the URLs a forge returns against the host it was configured
with (a pull request's ``html_url`` must sit under the configured GitHub), so
the proxy rewrites the upstream origin to its own in JSON and text response
bodies and in ``Location`` headers. Git pack data is passed through
untouched.

``serve`` generates a private test CA and a server certificate for ``--host``
in ``--state`` (only the CA's public certificate is meant to leave), listens
with TLS, and prints one JSON startup object with ``base_url`` (its own
origin) and ``ca_file``. Every request is forwarded to ``--upstream`` (an
origin) with the same path, query, method, body and headers (minus hop-by-hop
ones and ``Host``), verified against ``--upstream-ca`` or the system roots.

``fault`` arms a rule: the next ``--count`` requests with ``--method`` whose
path fully matches ``--path`` are answered ``--status`` with a GitHub-shaped
error body, without reaching the upstream. ``faults`` prints the armed rules
with what remains of each. Rules live in ``faults.json`` under the state
directory, read and consumed under a file lock, so the proxy and a caller
exec'ing into its Pod never race. Every request is logged as one JSON line:
method, path, status and whether a fault answered it. No header, body or
credential is ever logged.

Standard library only; it runs on the candidate worker image.
"""

from __future__ import annotations

import argparse
import contextlib
import fcntl
import http.client
import json
import os
import re
import ssl
import subprocess
import sys
import threading
import urllib.parse
from collections.abc import Iterator, Mapping
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any

FAULTS_FILE = "faults.json"
LOCK_FILE = "faults.lock"
CA_FILE = "ca.pem"
# Hop-by-hop headers (RFC 9110 7.6.1) and the ones this proxy recomputes.
_DROPPED = frozenset(
    {
        "connection",
        "keep-alive",
        "proxy-authenticate",
        "proxy-authorization",
        "proxy-connection",
        "te",
        "trailer",
        "transfer-encoding",
        "upgrade",
        "host",
        "content-length",
        "accept-encoding",
    }
)
_METHODS = ("GET", "HEAD", "POST", "PATCH", "PUT", "DELETE")
UPSTREAM_TIMEOUT_S = 60.0


# --------------------------------------------------------------------------
# Pure pieces
# --------------------------------------------------------------------------


def origin_of(url: str) -> str:
    """``scheme://host[:port]`` of ``url``, refusing anything but HTTP(S). Pure."""

    parts = urllib.parse.urlsplit(url)
    if parts.scheme not in ("http", "https") or not parts.hostname:
        raise ValueError(f"unsupported origin {url!r}")
    if parts.path not in ("", "/") or parts.query or parts.fragment or parts.username:
        raise ValueError(f"an origin carries no path, query or credentials: {url!r}")
    return f"{parts.scheme}://{parts.netloc}"


def upstream_url(upstream: str, raw_path: str) -> str:
    """The upstream origin joined with a request's path and query. Pure."""

    if not raw_path.startswith("/"):
        raise ValueError(f"a forwarded request path is absolute: {raw_path!r}")
    return upstream.rstrip("/") + raw_path


def rewrites_body(content_type: str | None) -> bool:
    """Whether a response body is text the origin rewrite applies to. Pure.

    JSON and text answers carry the forge's URLs; git pack data and every
    other binary type pass through byte for byte.
    """

    kind = (content_type or "").split(";", 1)[0].strip().lower()
    return kind.startswith("text/") or kind == "application/json" or kind.endswith("+json")


def rewrite_origin(data: bytes, upstream: str, public: str) -> bytes:
    """``data`` with every occurrence of the upstream origin replaced by ``public``. Pure."""

    return data.replace(upstream.encode(), public.encode())


def take_fault(rules: list[dict[str, Any]], method: str, path: str) -> int | None:
    """Consume one matching armed rule in place and return its status. Pure.

    ``path`` is the request path without its query. The first rule, in arming
    order, with remaining count, the same method and a full regex match wins.
    """

    for rule in rules:
        if (
            int(rule.get("remaining", 0)) > 0
            and str(rule.get("method", "")).upper() == method.upper()
            and re.fullmatch(str(rule.get("path", "")), path) is not None
        ):
            rule["remaining"] = int(rule["remaining"]) - 1
            rule["answered"] = int(rule.get("answered", 0)) + 1
            return int(rule["status"])
    return None


def forward_headers(headers: Mapping[str, str]) -> dict[str, str]:
    """Request or response headers with hop-by-hop and recomputed ones removed. Pure."""

    return {name: value for name, value in headers.items() if name.lower() not in _DROPPED}


def fault_body(status: int) -> bytes:
    """A GitHub-shaped error body for an injected answer. Pure."""

    return json.dumps(
        {"message": "Server Error", "status": str(status), "documentation_url": ""}
    ).encode()


# --------------------------------------------------------------------------
# Rule storage
# --------------------------------------------------------------------------


@contextlib.contextmanager
def _locked(state: Path) -> Iterator[Path]:
    state.mkdir(parents=True, exist_ok=True)
    with open(state / LOCK_FILE, "a+") as handle:
        fcntl.flock(handle, fcntl.LOCK_EX)
        try:
            yield state / FAULTS_FILE
        finally:
            fcntl.flock(handle, fcntl.LOCK_UN)


def _read(path: Path) -> list[dict[str, Any]]:
    try:
        rules = json.loads(path.read_text())
    except FileNotFoundError:
        return []
    if not isinstance(rules, list):
        raise ValueError(f"{path} does not hold a list of rules")
    return [rule for rule in rules if isinstance(rule, dict)]


def _write(path: Path, rules: list[dict[str, Any]]) -> None:
    temporary = path.with_suffix(".tmp")
    temporary.write_text(json.dumps(rules, sort_keys=True))
    os.replace(temporary, path)


def arm(state: Path, *, method: str, path: str, status: int, count: int) -> dict[str, Any]:
    """Append one rule; the returned rule is what was stored."""

    if method.upper() not in _METHODS:
        raise ValueError(f"unsupported method {method!r}")
    if not 500 <= status <= 599:
        raise ValueError("an injected failure answers a server error status")
    if count < 1:
        raise ValueError("a fault needs a positive count")
    re.compile(path)
    rule = {
        "method": method.upper(),
        "path": path,
        "status": status,
        "count": count,
        "remaining": count,
        "answered": 0,
    }
    with _locked(state) as faults:
        rules = _read(faults)
        rules.append(rule)
        _write(faults, rules)
    return rule


def armed(state: Path) -> list[dict[str, Any]]:
    with _locked(state) as faults:
        return _read(faults)


def consume(state: Path, method: str, path: str) -> int | None:
    with _locked(state) as faults:
        rules = _read(faults)
        status = take_fault(rules, method, path)
        if status is not None:
            _write(faults, rules)
        return status


# --------------------------------------------------------------------------
# Server
# --------------------------------------------------------------------------


def _openssl(*args: str) -> None:
    subprocess.run(["openssl", *args], check=True, capture_output=True, timeout=120)


def make_certificates(state: Path, host: str) -> tuple[Path, Path, Path]:
    """A private test CA and a server certificate for ``host``; keys stay in ``state``."""

    state.mkdir(parents=True, exist_ok=True)
    ca, ca_key = state / CA_FILE, state / "ca.key"
    server, server_key, csr = state / "server.pem", state / "server.key", state / "server.csr"
    _openssl(
        "req", "-x509", "-newkey", "rsa:2048", "-nodes", "-days", "2",
        "-subj", "/CN=Curie Forge Fault Proxy Test CA",
        "-keyout", str(ca_key), "-out", str(ca),
        "-addext", "basicConstraints=critical,CA:TRUE",
        "-addext", "keyUsage=critical,keyCertSign,cRLSign",
    )  # fmt: skip
    _openssl(
        "req", "-newkey", "rsa:2048", "-nodes", "-subj", f"/CN={host}",
        "-keyout", str(server_key), "-out", str(csr),
    )  # fmt: skip
    extension = state / "server.ext"
    address = re.fullmatch(r"[0-9.]+|[0-9a-fA-F:]+", host) is not None
    extension.write_text(
        f"subjectAltName={'IP' if address else 'DNS'}:{host}\n"
        "basicConstraints=CA:FALSE\n"
        "keyUsage=critical,digitalSignature,keyEncipherment\n"
        "extendedKeyUsage=serverAuth\n"
    )
    _openssl(
        "x509", "-req", "-in", str(csr), "-CA", str(ca), "-CAkey", str(ca_key),
        "-CAcreateserial", "-days", "2", "-out", str(server), "-extfile", str(extension),
    )  # fmt: skip
    for private in (ca_key, server_key):
        private.chmod(0o600)
    return ca, server, server_key


def _log(**fields: Any) -> None:
    print(json.dumps(fields, sort_keys=True), flush=True)


def _read_chunked(stream: Any) -> bytes:
    """A ``Transfer-Encoding: chunked`` request body (RFC 9112 7.1), trailers dropped."""

    body = bytearray()
    while True:
        size_line = stream.readline(1024)
        if not size_line:
            raise ValueError("the chunked body ended early")
        size = int(size_line.split(b";", 1)[0].strip(), 16)
        if size == 0:
            while stream.readline(1024) not in (b"\r\n", b"\n", b""):
                pass
            return bytes(body)
        body += stream.read(size)
        stream.readline(1024)


def build_handler(
    *, state: Path, upstream: str, public: str, upstream_context: ssl.SSLContext | None
) -> type[BaseHTTPRequestHandler]:
    """The request handler forwarding to the ``upstream`` origin, seen as ``public``."""

    upstream, public = origin_of(upstream), origin_of(public)
    lock = threading.Lock()

    class Handler(BaseHTTPRequestHandler):
        protocol_version = "HTTP/1.1"

        def log_message(self, format: str, *args: Any) -> None:  # noqa: A002 - stdlib name
            return

        def _answer(self, status: int, headers: Mapping[str, str], body: bytes) -> None:
            self.send_response(status)
            for name, value in headers.items():
                self.send_header(name, value)
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            if self.command != "HEAD":
                self.wfile.write(body)

        def _body(self) -> bytes:
            if "chunked" in (self.headers.get("Transfer-Encoding") or "").lower():
                return _read_chunked(self.rfile)
            length = int(self.headers.get("Content-Length") or 0)
            return self.rfile.read(length) if length else b""

        def _handle(self) -> None:
            body = self._body()
            path_only = urllib.parse.urlsplit(self.path).path
            with lock:
                status = consume(state, self.command, path_only)
            if status is not None:
                _log(
                    event="request", method=self.command, path=path_only, status=status, fault=True
                )
                self._answer(status, {"Content-Type": "application/json"}, fault_body(status))
                return
            url = urllib.parse.urlsplit(upstream_url(upstream, self.path))
            port = url.port or (443 if url.scheme == "https" else 80)
            connection: http.client.HTTPConnection
            if url.scheme == "https":
                connection = http.client.HTTPSConnection(
                    url.hostname or "", port, timeout=UPSTREAM_TIMEOUT_S, context=upstream_context
                )
            else:
                connection = http.client.HTTPConnection(
                    url.hostname or "", port, timeout=UPSTREAM_TIMEOUT_S
                )
            target_path = url.path + (f"?{url.query}" if url.query else "")
            try:
                connection.request(
                    self.command,
                    target_path,
                    body=body if body or self.command in ("POST", "PATCH", "PUT") else None,
                    headers={
                        **forward_headers(dict(self.headers.items())),
                        "Accept-Encoding": "identity",
                    },
                )
                response = connection.getresponse()
                answer = response.read()
                headers = forward_headers(dict(response.getheaders()))
                status = response.status
            except (OSError, http.client.HTTPException) as exc:
                _log(
                    event="request", method=self.command, path=path_only, status=502,
                    fault=False, upstream_error=type(exc).__name__,
                )  # fmt: skip
                self._answer(502, {"Content-Type": "application/json"}, fault_body(502))
                return
            finally:
                connection.close()
            encoded = any(name.lower() == "content-encoding" for name in headers)
            content_type = next(
                (value for name, value in headers.items() if name.lower() == "content-type"), None
            )
            if rewrites_body(content_type) and not encoded:
                answer = rewrite_origin(answer, upstream, public)
            for name in [name for name in headers if name.lower() == "location"]:
                headers[name] = headers[name].replace(upstream, public)
            _log(event="request", method=self.command, path=path_only, status=status, fault=False)
            self._answer(status, headers, answer)

        do_GET = _handle
        do_HEAD = _handle
        do_POST = _handle
        do_PATCH = _handle
        do_PUT = _handle
        do_DELETE = _handle

    return Handler


def serve(args: argparse.Namespace) -> int:
    state = Path(args.state)
    ca, certificate, key = make_certificates(state, args.host)
    upstream_context: ssl.SSLContext | None = None
    if urllib.parse.urlsplit(args.upstream).scheme == "https":
        upstream_context = ssl.create_default_context(cafile=args.upstream_ca or None)
    server = ThreadingHTTPServer((args.bind, args.port), BaseHTTPRequestHandler)
    public = f"https://{args.host}:{server.server_address[1]}"
    server.RequestHandlerClass = build_handler(
        state=state, upstream=args.upstream, public=public, upstream_context=upstream_context
    )
    context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    context.minimum_version = ssl.TLSVersion.TLSv1_2
    context.load_cert_chain(certificate, key)
    server.socket = context.wrap_socket(server.socket, server_side=True)
    _log(event="startup", base_url=public, ca_file=str(ca))
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()
    return 0


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    commands = parser.add_subparsers(dest="command", required=True)
    run = commands.add_parser("serve")
    run.add_argument("--state", required=True)
    run.add_argument("--host", required=True)
    run.add_argument("--bind", default="127.0.0.1")
    run.add_argument("--port", type=int, default=8443)
    run.add_argument("--upstream", required=True)
    run.add_argument("--upstream-ca", default="")
    rule = commands.add_parser("fault")
    rule.add_argument("--state", required=True)
    rule.add_argument("--method", required=True)
    rule.add_argument("--path", required=True)
    rule.add_argument("--status", type=int, default=502)
    rule.add_argument("--count", type=int, default=1)
    listing = commands.add_parser("faults")
    listing.add_argument("--state", required=True)
    args = parser.parse_args(argv)
    if args.command == "serve":
        return serve(args)
    if args.command == "fault":
        print(json.dumps(arm(Path(args.state), method=args.method, path=args.path,
                             status=args.status, count=args.count), sort_keys=True))  # fmt: skip
        return 0
    print(json.dumps(armed(Path(args.state)), sort_keys=True))
    return 0


if __name__ == "__main__":
    sys.exit(main())
