#!/usr/bin/env python3
"""The reference human-restricted hosted connector (ADR 0270 decision 5).

A small MCP server over streamable HTTP (``POST /mcp``, one JSON answer per
request) with one tool, ``record_effect``. Before every effect it:

1. Reads the requester only from the single ``X-Curie-Principal`` header its own
   caller proxy sets on its loopback forward (ADR 0270 decision 4). A request
   from any other peer, such as a caller on the connector's ``-direct``
   Service, never establishes a requester, whatever it sends. No requester
   refuses ``missing_requester``; a repeated or unreadable header refuses
   ``requester_invalid``. Tool arguments are never read for identity, so a
   model-written name cannot grant or redirect authority.
2. Asks current downstream authority: ``POST <AUTHORITY_URL>/authority`` with
   ``Authorization: Bearer <AUTHORITY_BEARER>`` and the principal as the JSON
   body, under one total timeout. Only a 200 whose body is exactly
   ``{"decision": "allow"}`` or ``{"decision": "deny"}`` is a decision. Deny
   refuses ``not_authorized``; anything else refuses ``authority_unavailable``.
   No earlier answer is cached.

A refusal is a tool result with ``isError`` true and the text
``refused <code>``. An effect appends to the in-memory ledger and answers
``effect=<n> requester=<fingerprint>``; the fingerprint is the first 16 hex
digits of the SHA-256 of the header value. The decoded principal lives only in
the handling request's locals, so no process global, connection, MCP session or
earlier request can supply it to another call. Each call writes one stderr line,
``record_effect outcome=<ok|code> requester=<fingerprint or ->``, and nothing
else about the requester is logged.

Standard library only, Python 3.12. The identity E2E harness ships this file's
text inline as a connectors.yaml env value (``identity_assets.reference_bundle``)
and the renderer substitutes dollar-brace placeholders in env values, so this
file must never contain one. Configured from the environment:
``AUTHORITY_URL``, ``AUTHORITY_BEARER`` and ``PORT`` (default 8000).
"""

from __future__ import annotations

import argparse
import base64
import binascii
import hashlib
import http.client
import ipaddress
import json
import math
import os
import re
import socket
import sys
import threading
import time
import urllib.parse
from collections.abc import Mapping
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any

PRINCIPAL_HEADER = "X-Curie-Principal"
TOOL_NAME = "record_effect"
SERVER_NAME = "human-restricted-connector"
SERVER_VERSION = "1.0.0"
MCP_PATH = "/mcp"
AUTHORITY_PATH = "/authority"
DEFAULT_PORT = 8000

OK = "ok"
MISSING_REQUESTER = "missing_requester"
REQUESTER_INVALID = "requester_invalid"
AUTHORITY_UNAVAILABLE = "authority_unavailable"
NOT_AUTHORIZED = "not_authorized"

# The header reader's bounds, the same as the caller proxy's (ADR 0270 decision 4).
MAX_HEADER_CHARS = 32 * 1024
ISSUER_MAX_CHARS = 2048
SUBJECT_MAX_CHARS = 256
_PRINCIPAL_FIELDS = frozenset({"tenant_id", "issuer", "subject"})
_URL_SAFE = re.compile(r"[A-Za-z0-9_-]+")
_UUID = re.compile(r"[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}")
_CONTENT_LENGTH = re.compile(r"[0-9]{1,9}")

MAX_REQUEST_BYTES = 1024 * 1024
MAX_AUTHORITY_BYTES = 4096
# Echoed when the client asks for one of these; otherwise the first is offered.
PROTOCOL_VERSIONS = ("2025-11-25", "2025-06-18", "2025-03-26", "2024-11-05")

PARSE_ERROR = -32700
INVALID_REQUEST = -32600
METHOD_NOT_FOUND = -32601
INVALID_PARAMS = -32602

TOOL: dict[str, Any] = {
    "name": TOOL_NAME,
    "description": (
        "Record one synthetic effect as the verified requester. The requester comes "
        "only from the caller proxy; no argument names, grants or changes it."
    ),
    "inputSchema": {
        "type": "object",
        "properties": {"note": {"type": "string", "description": "A free-text note."}},
    },
}


# --- the principal header reader -----------------------------------------------------


class _Refused(ValueError):
    """JSON text the strict readers refuse."""


def _unique_object(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    # A first-wins and a last-wins reader would each see a different value.
    keys = [key for key, _ in pairs]
    if len(keys) != len(set(keys)):
        raise _Refused("duplicate key")
    return dict(pairs)


def _no_constant(name: str) -> Any:
    raise _Refused(f"{name} is not JSON")


def _strict_json(raw: bytes) -> Any:
    """Strict UTF-8 and strict JSON: no duplicate key, NaN or Infinity. Raises ValueError."""

    try:
        return json.loads(
            raw.decode("utf-8"), object_pairs_hook=_unique_object, parse_constant=_no_constant
        )
    except RecursionError:
        raise _Refused("nested too deeply") from None


def _b64url(raw: bytes) -> str:
    return base64.urlsafe_b64encode(raw).rstrip(b"=").decode("ascii")


def _bounded_text(value: object, max_chars: int) -> bool:
    if not isinstance(value, str) or not 1 <= len(value) <= max_chars:
        return False
    try:
        value.encode("utf-8")
    except UnicodeEncodeError:
        return False
    return True


def principal_header_value(principal: Mapping[str, str]) -> str:
    """The one spelling of a principal: compact sorted-key UTF-8 JSON, unpadded base64url."""

    text = json.dumps(
        {field: principal[field] for field in _PRINCIPAL_FIELDS},
        separators=(",", ":"),
        sort_keys=True,
        ensure_ascii=False,
    )
    return _b64url(text.encode("utf-8"))


def read_principal_header(value: object) -> dict[str, str] | None:
    """The principal one ``X-Curie-Principal`` value carries, or None. Never raises.

    The length bound comes before any decoding. The value must be unpadded
    base64url of strict UTF-8 JSON: one object, no duplicate key, no NaN or
    Infinity, exactly ``tenant_id`` (lowercase hyphenated UUID), ``issuer`` (1
    to 2048 characters) and ``subject`` (1 to 256 characters). Re-encoding the
    principal must give the value back, which refuses every second spelling
    (stray trailing bits, unsorted keys, spacing, escapes, a byte order mark).
    """

    if not isinstance(value, str) or not 0 < len(value) <= MAX_HEADER_CHARS:
        return None
    if not value.isascii() or _URL_SAFE.fullmatch(value) is None:
        return None
    try:
        parsed = _strict_json(base64.urlsafe_b64decode(value + "=" * (-len(value) % 4)))
    except (binascii.Error, ValueError):
        return None
    if not isinstance(parsed, dict) or set(parsed) != _PRINCIPAL_FIELDS:
        return None
    tenant_id, issuer, subject = parsed["tenant_id"], parsed["issuer"], parsed["subject"]
    if not isinstance(tenant_id, str) or _UUID.fullmatch(tenant_id) is None:
        return None
    if not _bounded_text(issuer, ISSUER_MAX_CHARS) or not _bounded_text(subject, SUBJECT_MAX_CHARS):
        return None
    principal = {"tenant_id": tenant_id, "issuer": issuer, "subject": subject}
    if principal_header_value(principal) != value:
        return None
    return principal


def from_caller_proxy(host: str) -> bool:
    """Whether a request's peer address is the caller proxy's loopback forward.

    The proxy forwards to this server over loopback inside the pod; any other
    peer reached the server around the proxy and attests nothing.
    """

    try:
        address = ipaddress.ip_address(host)
    except ValueError:
        return False
    mapped = address.ipv4_mapped if isinstance(address, ipaddress.IPv6Address) else None
    return (mapped or address).is_loopback


def fingerprint(header: str) -> str:
    """What the ledger and the log carry instead of the principal."""

    return hashlib.sha256(header.encode("ascii")).hexdigest()[:16]


# --- current authority ---------------------------------------------------------------


def _cut(sock: socket.socket) -> None:
    try:
        sock.shutdown(socket.SHUT_RDWR)
    except OSError:
        pass


class Authority:
    """``POST <url>/authority`` for one principal: ``allow``, ``deny`` or None."""

    def __init__(self, url: str, bearer: str, timeout_s: float) -> None:
        parts = urllib.parse.urlsplit(url)
        if parts.scheme not in {"http", "https"} or not parts.hostname:
            raise ValueError("the authority URL must be an http or https URL with a host")
        if parts.username or parts.password or parts.query or parts.fragment:
            raise ValueError("the authority URL carries no credentials, query or fragment")
        port = parts.port  # ValueError for a malformed port
        if (
            not isinstance(bearer, str)
            or not bearer
            or not bearer.isascii()
            or not bearer.isprintable()
            or any(character.isspace() for character in bearer)
        ):
            raise ValueError("the authority bearer must be non-empty printable ASCII")
        if not isinstance(timeout_s, int | float) or not math.isfinite(timeout_s):
            raise ValueError("the authority timeout must be a finite number of seconds")
        if timeout_s <= 0:
            raise ValueError("the authority timeout must be positive")
        self._https = parts.scheme == "https"
        self._host = parts.hostname
        self._port = port
        self._path = parts.path.rstrip("/") + AUTHORITY_PATH
        self._authorization = "Bearer " + bearer
        self._timeout_s = float(timeout_s)

    def _connection(self) -> http.client.HTTPConnection:
        if self._https:
            return http.client.HTTPSConnection(self._host, self._port, timeout=self._timeout_s)
        return http.client.HTTPConnection(self._host, self._port, timeout=self._timeout_s)

    def decide(self, principal: Mapping[str, str]) -> str | None:
        """The current decision, or None when no decision arrived within the timeout."""

        body = json.dumps(dict(principal), separators=(",", ":"), sort_keys=True).encode("ascii")
        deadline = time.monotonic() + self._timeout_s
        connection = self._connection()
        timer: threading.Timer | None = None
        try:
            connection.connect()
            remaining = deadline - time.monotonic()
            if remaining <= 0 or connection.sock is None:
                return None
            connection.sock.settimeout(remaining)
            # One total bound: whatever is still in flight at the deadline is cut off.
            timer = threading.Timer(remaining, _cut, (connection.sock,))
            timer.daemon = True
            timer.start()
            connection.request(
                "POST",
                self._path,
                body=body,
                headers={
                    "Authorization": self._authorization,
                    "Content-Type": "application/json",
                    "Accept": "application/json",
                    "Connection": "close",
                },
            )
            response = connection.getresponse()
            status = response.status
            raw = response.read(MAX_AUTHORITY_BYTES + 1)
        except (OSError, http.client.HTTPException, ValueError):
            return None
        finally:
            if timer is not None:
                timer.cancel()
            connection.close()
        if time.monotonic() > deadline or status != 200 or len(raw) > MAX_AUTHORITY_BYTES:
            return None
        try:
            answer = _strict_json(raw)
        except ValueError:
            return None
        if not isinstance(answer, dict) or set(answer) != {"decision"}:
            return None
        decision = answer["decision"]
        return decision if decision in ("allow", "deny") else None


# --- the MCP server ------------------------------------------------------------------


def _log(outcome: str, requester: str | None) -> None:
    try:
        sys.stderr.write(f"{TOOL_NAME} outcome={outcome} requester={requester or '-'}\n")
        sys.stderr.flush()
    except (AttributeError, OSError, ValueError):
        pass


class HumanRestrictedServer(ThreadingHTTPServer):
    """One thread per connection; ``ledger`` holds the applied effects in order."""

    daemon_threads = True
    # A keep-alive connection a client left open must not hold up server_close.
    block_on_close = False

    def __init__(self, address: tuple[str, int], authority: Authority) -> None:
        super().__init__(address, _Handler)
        self.authority = authority
        self.ledger: list[dict[str, Any]] = []
        self._ledger_lock = threading.Lock()

    def apply_effect(self, requester: str) -> int:
        with self._ledger_lock:
            n = len(self.ledger) + 1
            self.ledger.append({"n": n, "requester": requester})
            return n

    def handle_error(self, request: Any, client_address: Any) -> None:
        # One line, never a traceback with request data in it.
        error = sys.exc_info()[1]
        try:
            sys.stderr.write(f"{SERVER_NAME}: request failed: {type(error).__name__}\n")
        except (AttributeError, OSError, ValueError):
            pass


class _Handler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"
    server_version = SERVER_NAME
    sys_version = ""
    server: HumanRestrictedServer

    def log_message(self, format: str, *args: Any) -> None:  # noqa: A002
        # No access log: the outcome line per tool call is the only output.
        return

    # --- responses

    def _send(
        self, status: int, raw: bytes, *, content_type: str | None, close: bool = False
    ) -> None:
        self.send_response(status)
        if content_type is not None:
            self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(raw)))
        self.send_header("Cache-Control", "no-store")
        if status == 405:
            self.send_header("Allow", "POST")
        if close:
            self.send_header("Connection", "close")
        self.end_headers()
        if raw:
            self.wfile.write(raw)

    def _json(self, status: int, payload: Mapping[str, Any], *, close: bool = False) -> None:
        raw = json.dumps(payload, separators=(",", ":"), ensure_ascii=False).encode("utf-8")
        self._send(status, raw, content_type="application/json", close=close)

    def _result(self, request_id: str | int, result: Mapping[str, Any]) -> None:
        self._json(200, {"jsonrpc": "2.0", "id": request_id, "result": result})

    def _error(
        self,
        status: int,
        request_id: str | int | None,
        code: int,
        message: str,
        *,
        close: bool = False,
    ) -> None:
        error = {"jsonrpc": "2.0", "id": request_id, "error": {"code": code, "message": message}}
        self._json(status, error, close=close)

    # --- methods

    def _on_mcp(self) -> bool:
        return self.path.split("?", 1)[0] == MCP_PATH

    def _not_post(self) -> None:
        self._send(405 if self._on_mcp() else 404, b"", content_type=None, close=True)

    do_GET = _not_post  # noqa: N815 - a stream for server-sent messages is not offered
    do_DELETE = _not_post  # noqa: N815 - no session is issued, so none ends
    do_PUT = _not_post  # noqa: N815
    do_PATCH = _not_post  # noqa: N815

    def _body(self) -> bytes | None:
        if self.headers.get("Transfer-Encoding") is not None:
            return None
        lengths = self.headers.get_all("Content-Length") or []
        if len(lengths) != 1 or _CONTENT_LENGTH.fullmatch(lengths[0]) is None:
            return None
        length = int(lengths[0])
        if not 0 < length <= MAX_REQUEST_BYTES:
            return None
        raw = self.rfile.read(length)
        return raw if len(raw) == length else None

    def do_POST(self) -> None:  # noqa: N802
        if not self._on_mcp():
            self._send(404, b"", content_type=None, close=True)
            return
        raw = self._body()
        if raw is None:
            self._error(400, None, INVALID_REQUEST, "a request is one framed JSON body", close=True)
            return
        try:
            message = _strict_json(raw)
        except ValueError:
            self._error(400, None, PARSE_ERROR, "the body is not JSON")
            return
        if not isinstance(message, dict) or message.get("jsonrpc") != "2.0":
            self._error(400, None, INVALID_REQUEST, "one JSON-RPC 2.0 message per request")
            return
        if "method" not in message:
            # A response to a server request. This server sends none, so accept and drop it.
            if "id" in message and ("result" in message or "error" in message):
                self._send(202, b"", content_type=None)
            else:
                self._error(400, None, INVALID_REQUEST, "neither a request nor a response")
            return
        method = message["method"]
        if not isinstance(method, str):
            self._error(400, None, INVALID_REQUEST, "the method is not a string")
            return
        if "id" not in message:
            self._send(202, b"", content_type=None)  # a notification
            return
        request_id = message["id"]
        if isinstance(request_id, bool) or not isinstance(request_id, str | int):
            self._error(400, None, INVALID_REQUEST, "the id is not a string or an integer")
            return
        params = message.get("params", {})
        if params is None:
            params = {}
        if not isinstance(params, dict):
            self._error(200, request_id, INVALID_PARAMS, "params is not an object")
            return
        if method == "initialize":
            self._initialize(request_id, params)
        elif method == "ping":
            self._result(request_id, {})
        elif method == "tools/list":
            self._result(request_id, {"tools": [TOOL]})
        elif method == "tools/call":
            self._tools_call(request_id, params)
        else:
            self._error(200, request_id, METHOD_NOT_FOUND, "method not found")

    def _initialize(self, request_id: str | int, params: Mapping[str, Any]) -> None:
        requested = params.get("protocolVersion")
        if not isinstance(requested, str):
            self._error(200, request_id, INVALID_PARAMS, "protocolVersion is not a string")
            return
        version = requested if requested in PROTOCOL_VERSIONS else PROTOCOL_VERSIONS[0]
        self._result(
            request_id,
            {
                "protocolVersion": version,
                "capabilities": {"tools": {}},
                "serverInfo": {"name": SERVER_NAME, "version": SERVER_VERSION},
            },
        )

    def _tools_call(self, request_id: str | int, params: Mapping[str, Any]) -> None:
        if params.get("name") != TOOL_NAME:
            self._error(200, request_id, INVALID_PARAMS, "unknown tool")
            return
        arguments = params.get("arguments")
        if arguments is None:
            arguments = {}
        if not isinstance(arguments, dict) or not isinstance(arguments.get("note", ""), str):
            self._error(200, request_id, INVALID_PARAMS, "arguments take an optional string note")
            return
        # The arguments are checked for shape and otherwise unused: nothing a model
        # writes names, grants or redirects the requester.
        outcome, requester, n = self._record_effect()
        _log(outcome, requester)
        if outcome == OK:
            text, is_error = f"effect={n} requester={requester}", False
        else:
            text, is_error = f"refused {outcome}", True
        self._result(request_id, {"content": [{"type": "text", "text": text}], "isError": is_error})

    def _record_effect(self) -> tuple[str, str | None, int | None]:
        """``(outcome, requester fingerprint, effect number)`` for this request alone."""

        if not from_caller_proxy(str(self.client_address[0])):
            return MISSING_REQUESTER, None, None
        values = self.headers.get_all(PRINCIPAL_HEADER) or []
        if not values:
            return MISSING_REQUESTER, None, None
        if len(values) != 1:
            return REQUESTER_INVALID, None, None
        header = values[0]
        principal = read_principal_header(header)
        if principal is None:
            return REQUESTER_INVALID, None, None
        requester = fingerprint(header)
        decision = self.server.authority.decide(principal)
        if decision is None:
            return AUTHORITY_UNAVAILABLE, requester, None
        if decision != "allow":
            return NOT_AUTHORIZED, requester, None
        return OK, requester, self.server.apply_effect(requester)


def make_server(
    host: str,
    port: int,
    *,
    authority_url: str,
    authority_bearer: str,
    timeout_s: float = 2.0,
) -> ThreadingHTTPServer:
    """The connector bound to ``(host, port)``, not yet serving. ValueError on bad config."""

    authority = Authority(authority_url, authority_bearer, timeout_s)
    return HumanRestrictedServer((host, port), authority)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        prog="human_restricted_connector",
        description=(
            "The reference human-restricted hosted connector. Configured by AUTHORITY_URL, "
            f"AUTHORITY_BEARER and PORT (default {DEFAULT_PORT})."
        ),
    )
    parser.parse_args(argv)
    port_text = os.environ.get("PORT", str(DEFAULT_PORT))
    try:
        port = int(port_text)
        if not 0 < port < 65536:
            raise ValueError("PORT is not a TCP port")
        server = make_server(
            "0.0.0.0",  # noqa: S104 - the connector Service routes here
            port,
            authority_url=os.environ.get("AUTHORITY_URL", ""),
            authority_bearer=os.environ.get("AUTHORITY_BEARER", ""),
        )
    except ValueError as exc:
        sys.stderr.write(f"{SERVER_NAME}: {exc}\n")
        return 2
    sys.stderr.write(f"{SERVER_NAME}: serving MCP on :{port}{MCP_PATH}\n")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
