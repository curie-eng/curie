#!/usr/bin/env python3
"""A scripted HTTP identity provider and email reply sink for `curie dev identity-e2e`.

Standard library only, so it runs from a ConfigMap under any image with a
Python interpreter (the harness uses the API image). Two listeners:

1. HTTPS lookup (``--lookup-port``): ``POST /lookup`` with
   ``Authorization: Bearer <bearer>``. The API's provider client is the only
   caller. Every answer is a deep copy of a recorded response from the
   committed lookup vectors (``tests/vectors/http-identity-v1.json``), never a
   shape built from the API's parser. The provider changes only what a real
   directory would: it echoes the request's ``schema_version``,
   ``request_id``, ``purpose``, ``authority_version`` and ``scope``, puts the
   roster's issuer, subject and groups in, and stamps the observations with
   the current time.
2. Plain HTTP (``--sink-port``): ``POST /reply/<label>`` is the email reply
   endpoint the harness binds; it requires the worker's
   ``X-Curie-Adapter-Secret`` and records the event. ``GET /records`` returns
   what was recorded (lookup scopes and outcomes, reply events, authority
   decisions and admin changes), never a bearer or adapter secret.
   ``GET /healthz`` answers readiness.

The sink listener also serves current downstream authority for the reference
human-restricted connector (ADR 0270 decision 5), when an authority bearer is
configured (``--authority-bearer-file``; otherwise ``/authority`` is 404):

1. ``POST /authority`` with ``Authorization: Bearer <authority bearer>`` and a
   principal ``{"tenant_id", "issuer", "subject"}`` answers
   ``{"decision": "allow"}`` only for the configured ``--tenant``, the roster
   issuer, and exactly one roster sender with that subject who is an active
   human in ``--authority-group`` (default ``identity-e2e-members``); any other
   principal is ``{"decision": "deny"}``. 401 without the bearer, 503 while
   the authority is unavailable.
2. ``POST /admin/membership`` ``{"sender", "group", "member"}`` adds or removes
   one roster sender's group, and ``POST /admin/authority`` ``{"available"}``
   takes the authority down or brings it back. Both require the adapter
   secret. A membership change is a roster change, so lookups see it too.

The roster maps a lowercased sender address to a scripted identity::

    {
      "issuer": "https://identity-e2e.example.test/directory",
      "unknown": "unresolved",
      "senders": {
        "alice@example.test": {
          "subject": "identity-e2e-alice",
          "sender_kind": "human",
          "account_status": "active",
          "groups": ["identity-e2e-members"]
        }
      }
    }

``account_status`` ``active`` resolves, ``disabled`` or ``revoked`` is
``inactive``; an entry may instead name ``"outcome": "unresolved"`` or
``"unavailable"``. A sender missing from the roster gets ``unknown``.
"""

from __future__ import annotations

import argparse
import copy
import hmac
import json
import re
import ssl
import sys
import threading
from collections.abc import Callable, Mapping
from datetime import UTC, datetime, timedelta
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any

MAX_BODY = 65_536
MAX_RECORDS = 500
ECHOED = ("schema_version", "request_id", "purpose", "authority_version", "scope")
ADAPTER_SECRET_HEADER = "X-Curie-Adapter-Secret"
OUTCOMES = frozenset({"resolved", "inactive", "unresolved", "unavailable"})
DEFAULT_AUTHORITY_GROUP = "identity-e2e-members"
PRINCIPAL_FIELDS = frozenset({"tenant_id", "issuer", "subject"})
_TENANT = re.compile(r"[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}")


class RosterError(ValueError):
    """The roster or vectors cannot script an answer."""


def _stamp(now: datetime) -> str:
    # A fresh observation a little in the past, as a real directory reports.
    return (now - timedelta(milliseconds=20)).isoformat()


def _outcome(entry: Mapping[str, Any]) -> str:
    explicit = entry.get("outcome")
    if explicit is not None:
        if explicit not in OUTCOMES:
            raise RosterError(f"unknown outcome {explicit!r}")
        return str(explicit)
    status = entry.get("account_status", "active")
    if status == "active":
        return "resolved"
    if status in {"disabled", "revoked"}:
        return "inactive"
    raise RosterError(f"unknown account_status {status!r}")


def template_for(
    vectors: Mapping[str, Any], outcome: str, sender_kind: str, account_status: str | None
) -> dict[str, Any]:
    """The recorded response whose outcome, sender kind and status match."""

    for case in vectors.get("cases", ()):
        response = case.get("response")
        if not isinstance(response, dict):
            continue
        if response.get("outcome") != outcome or response.get("sender_kind") != sender_kind:
            continue
        identity = response.get("identity")
        if account_status is not None and isinstance(identity, dict):
            if identity.get("account_status") != account_status:
                continue
        return copy.deepcopy(response)
    raise RosterError(
        f"no recorded lookup response for outcome={outcome} sender_kind={sender_kind} "
        f"account_status={account_status}"
    )


def answer(
    request: Mapping[str, Any],
    roster: Mapping[str, Any],
    vectors: Mapping[str, Any],
    *,
    now: datetime | None = None,
) -> dict[str, Any]:
    """The scripted lookup response for one parsed request."""

    scope = request["scope"]
    native_id = str(scope["sender"]["native_id"]).lower()
    senders = roster.get("senders", {})
    entry = senders.get(native_id)
    if entry is None:
        outcome = str(roster.get("unknown", "unresolved"))
        if outcome not in {"unresolved", "unavailable"}:
            raise RosterError("an unknown sender is unresolved or unavailable")
        response = template_for(vectors, outcome, "unknown", None)
        entry = {}
    else:
        outcome = _outcome(entry)
        kind = str(entry.get("sender_kind", "human"))
        status = (
            None
            if outcome in {"unresolved", "unavailable"}
            else str(entry.get("account_status", "active"))
        )
        response = template_for(vectors, outcome, kind, status)
    for key in ECHOED:
        response[key] = copy.deepcopy(request[key])
    issuer = roster.get("issuer") or scope["expected_issuer"]
    stamp = _stamp(now or datetime.now(UTC))
    identity = response.get("identity")
    if isinstance(identity, dict):
        identity["issuer"] = issuer
        identity["subject"] = str(entry.get("subject", identity.get("subject")))
    observation = response.get("identity_observation")
    if isinstance(observation, dict):
        observation.update(as_of=stamp, max_source_lag_ms=0)
    membership = response.get("membership")
    if isinstance(membership, dict):
        groups = entry.get("groups")
        if groups is not None:
            membership["groups"] = [{"issuer": issuer, "id": str(group)} for group in groups]
        else:
            for group in membership.get("groups") or ():
                group["issuer"] = issuer
        member_observation = membership.get("observation")
        if isinstance(member_observation, dict):
            member_observation.update(as_of=stamp, max_source_lag_ms=0)
    return response


class Provider:
    """Shared state for both listeners."""

    def __init__(
        self,
        *,
        roster: Mapping[str, Any],
        vectors: Mapping[str, Any],
        bearer: str,
        adapter_secret: str,
        authority_bearer: str | None = None,
        tenant: str | None = None,
        authority_group: str = DEFAULT_AUTHORITY_GROUP,
    ) -> None:
        if not bearer or not adapter_secret:
            raise RosterError("the bearer and adapter secret must be non-empty")
        if authority_bearer is not None and (
            not authority_bearer or authority_bearer in {bearer, adapter_secret}
        ):
            raise RosterError("the authority bearer must be non-empty and its own secret")
        if tenant is not None and _TENANT.fullmatch(tenant) is None:
            raise RosterError("the tenant must be a lowercase hyphenated UUID")
        if not authority_group:
            raise RosterError("the authority group must be non-empty")
        self.roster = roster
        self.vectors = vectors
        self.tenant = tenant
        self.authority_group = authority_group
        self._bearer = bearer
        self._adapter_secret = adapter_secret
        self._authority_bearer = authority_bearer
        self._authority_available = True
        self._lock = threading.Lock()
        self._records: list[dict[str, Any]] = []

    def record(self, entry: dict[str, Any]) -> None:
        entry["at"] = datetime.now(UTC).isoformat()
        with self._lock:
            self._records.append(entry)
            del self._records[:-MAX_RECORDS]

    def records(self) -> list[dict[str, Any]]:
        with self._lock:
            return copy.deepcopy(self._records)

    def bearer_ok(self, header: str | None) -> bool:
        expected = "Bearer " + self._bearer
        return header is not None and hmac.compare_digest(
            header.encode("utf-8"), expected.encode("utf-8")
        )

    def adapter_ok(self, header: str | None) -> bool:
        return header is not None and hmac.compare_digest(
            header.encode("utf-8"), self._adapter_secret.encode("utf-8")
        )

    @property
    def serves_authority(self) -> bool:
        return self._authority_bearer is not None

    def authority_bearer_ok(self, header: str | None) -> bool:
        if header is None or self._authority_bearer is None:
            return False
        expected = "Bearer " + self._authority_bearer
        return hmac.compare_digest(header.encode("utf-8"), expected.encode("utf-8"))

    @property
    def authority_available(self) -> bool:
        with self._lock:
            return self._authority_available

    def set_authority_available(self, available: bool) -> None:
        with self._lock:
            self._authority_available = available

    def set_membership(self, sender: str, group: str, member: bool) -> bool:
        """Add or remove one roster sender's group; False when the sender is unknown."""

        with self._lock:
            entry = self.roster.get("senders", {}).get(sender.lower())
            if not isinstance(entry, dict):
                return False
            groups = [str(g) for g in entry.get("groups") or () if str(g) != group]
            # A new list, so a lookup reading the old one concurrently sees a whole roster.
            entry["groups"] = [*groups, group] if member else groups
            return True

    def decide(self, principal: Mapping[str, str]) -> str:
        """``allow`` or ``deny`` for one principal, from the roster as it is now."""

        with self._lock:
            if self.tenant is None or principal["tenant_id"] != self.tenant:
                return "deny"
            if principal["issuer"] != self.roster.get("issuer"):
                return "deny"
            matches = [
                entry
                for entry in self.roster.get("senders", {}).values()
                if isinstance(entry, dict) and entry.get("subject") == principal["subject"]
            ]
            if len(matches) != 1:
                return "deny"
            entry = matches[0]
            try:
                active = _outcome(entry) == "resolved"
            except RosterError:
                active = False
            human = entry.get("sender_kind", "human") == "human"
            member = self.authority_group in [str(g) for g in entry.get("groups") or ()]
            return "allow" if active and human and member else "deny"


def _handler(provider: Provider, *, lookup: bool) -> type[BaseHTTPRequestHandler]:
    class Handler(BaseHTTPRequestHandler):
        protocol_version = "HTTP/1.1"
        server_version = "identity-e2e-provider"

        def log_message(self, format: str, *args: Any) -> None:  # noqa: A002
            # Request lines only; headers never reach the log.
            sys.stderr.write(f"{self.address_string()} {format % args}\n")

        def _send(self, status: int, body: Any) -> None:
            raw = json.dumps(body, separators=(",", ":")).encode("utf-8")
            self.send_response(status)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(raw)))
            self.end_headers()
            self.wfile.write(raw)

        def _raw(self) -> bytes | None:
            """The whole request body, read before any answer so keep-alive stays framed."""

            try:
                length = int(self.headers.get("Content-Length") or 0)
            except ValueError:
                length = -1
            if not 0 < length <= MAX_BODY:
                self.close_connection = True
                return None
            return self.rfile.read(length)

        def do_GET(self) -> None:  # noqa: N802
            if self.path == "/healthz":
                self._send(200, {"ok": True})
            elif self.path == "/records" and not lookup:
                self._send(200, {"records": provider.records()})
            else:
                self._send(404, {"error": "not_found"})

        def do_POST(self) -> None:  # noqa: N802
            raw = self._raw()
            if raw is None:
                self._send(400, {"error": "bad_request"})
            elif lookup and self.path == "/lookup":
                self._lookup(raw)
            elif not lookup and self.path.startswith("/reply/"):
                self._reply(self.path.removeprefix("/reply/"), raw)
            elif not lookup and self.path == "/authority":
                self._authority(raw)
            elif not lookup and self.path in {"/admin/membership", "/admin/authority"}:
                self._admin(self.path, raw)
            else:
                self._send(404, {"error": "not_found"})

        def _lookup(self, raw: bytes) -> None:
            if not provider.bearer_ok(self.headers.get("Authorization")):
                provider.record({"type": "lookup", "status": 401})
                self._send(401, {"error": "unauthorized"})
                return
            try:
                request = json.loads(raw)
                response = answer(request, provider.roster, provider.vectors)
            except (ValueError, KeyError, TypeError) as exc:
                provider.record({"type": "lookup", "status": 400, "error": type(exc).__name__})
                self._send(400, {"error": "bad_request"})
                return
            provider.record(
                {
                    "type": "lookup",
                    "status": 200,
                    "purpose": request.get("purpose"),
                    "scope": request.get("scope"),
                    "outcome": response.get("outcome"),
                    "subject": (response.get("identity") or {}).get("subject"),
                }
            )
            self._send(200, response)

        def _reply(self, label: str, raw: bytes) -> None:
            if not provider.adapter_ok(self.headers.get(ADAPTER_SECRET_HEADER)):
                provider.record({"type": "reply", "label": label, "status": 401})
                self._send(401, {"error": "unauthorized"})
                return
            try:
                event = json.loads(raw)
            except ValueError:
                self._send(400, {"error": "bad_request"})
                return
            provider.record({"type": "reply", "label": label, "status": 200, "event": event})
            self._send(200, {"ok": True})

        def _authority(self, raw: bytes) -> None:
            if not provider.serves_authority:
                provider.record({"type": "authority", "status": 404})
                self._send(404, {"error": "not_found"})
                return
            if not provider.authority_bearer_ok(self.headers.get("Authorization")):
                provider.record({"type": "authority", "status": 401})
                self._send(401, {"error": "unauthorized"})
                return
            if not provider.authority_available:
                provider.record({"type": "authority", "status": 503})
                self._send(503, {"error": "unavailable"})
                return
            try:
                principal = json.loads(raw)
            except ValueError:
                principal = None
            if (
                not isinstance(principal, dict)
                or set(principal) != PRINCIPAL_FIELDS
                or not all(isinstance(value, str) for value in principal.values())
            ):
                provider.record({"type": "authority", "status": 400})
                self._send(400, {"error": "bad_request"})
                return
            decision = provider.decide(principal)
            provider.record(
                {
                    "type": "authority",
                    "status": 200,
                    "subject": principal["subject"],
                    "decision": decision,
                }
            )
            self._send(200, {"decision": decision})

        def _admin(self, path: str, raw: bytes) -> None:
            if not provider.adapter_ok(self.headers.get(ADAPTER_SECRET_HEADER)):
                provider.record({"type": "admin", "path": path, "status": 401})
                self._send(401, {"error": "unauthorized"})
                return
            try:
                change = json.loads(raw)
            except ValueError:
                change = None
            if path == "/admin/authority":
                if not isinstance(change, dict) or set(change) != {"available"}:
                    change = None
                elif not isinstance(change["available"], bool):
                    change = None
                if change is None:
                    provider.record({"type": "admin", "path": path, "status": 400})
                    self._send(400, {"error": "bad_request"})
                    return
                provider.set_authority_available(change["available"])
                provider.record(
                    {"type": "admin", "path": path, "status": 200, "available": change["available"]}
                )
                self._send(200, {"ok": True})
                return
            if (
                not isinstance(change, dict)
                or set(change) != {"sender", "group", "member"}
                or not isinstance(change["sender"], str)
                or not isinstance(change["group"], str)
                or not change["group"]
                or not isinstance(change["member"], bool)
            ):
                provider.record({"type": "admin", "path": path, "status": 400})
                self._send(400, {"error": "bad_request"})
                return
            sender, group, member = change["sender"].lower(), change["group"], change["member"]
            if not provider.set_membership(sender, group, member):
                provider.record({"type": "admin", "path": path, "status": 404, "sender": sender})
                self._send(404, {"error": "unknown_sender"})
                return
            provider.record(
                {
                    "type": "admin",
                    "path": path,
                    "status": 200,
                    "sender": sender,
                    "group": group,
                    "member": member,
                }
            )
            self._send(200, {"ok": True})

    return Handler


def serve(
    provider: Provider,
    *,
    host: str,
    lookup_port: int,
    sink_port: int,
    tls: ssl.SSLContext,
) -> tuple[ThreadingHTTPServer, ThreadingHTTPServer]:
    """Start both listeners on daemon threads and return the servers."""

    lookup_server = ThreadingHTTPServer((host, lookup_port), _handler(provider, lookup=True))
    lookup_server.socket = tls.wrap_socket(lookup_server.socket, server_side=True)
    sink_server = ThreadingHTTPServer((host, sink_port), _handler(provider, lookup=False))
    for server in (lookup_server, sink_server):
        # A short poll keeps shutdown() quick for the harness tests.
        threading.Thread(
            target=server.serve_forever, kwargs={"poll_interval": 0.05}, daemon=True
        ).start()
    return lookup_server, sink_server


def server_tls(cert_file: str, key_file: str) -> ssl.SSLContext:
    context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    context.minimum_version = ssl.TLSVersion.TLSv1_2
    context.load_cert_chain(cert_file, key_file)
    return context


def _read(path: str) -> str:
    with open(path, encoding="utf-8") as handle:
        return handle.read().strip()


def main(argv: list[str] | None = None, *, wait: Callable[[], None] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--roster", required=True)
    parser.add_argument("--vectors", required=True)
    parser.add_argument("--bearer-file", required=True)
    parser.add_argument("--adapter-secret-file", required=True)
    parser.add_argument("--authority-bearer-file")
    parser.add_argument("--tenant")
    parser.add_argument("--authority-group", default=DEFAULT_AUTHORITY_GROUP)
    parser.add_argument("--cert", required=True)
    parser.add_argument("--key", required=True)
    parser.add_argument("--host", default="0.0.0.0")  # noqa: S104 - in-cluster Service target
    parser.add_argument("--lookup-port", type=int, default=8443)
    parser.add_argument("--sink-port", type=int, default=8080)
    args = parser.parse_args(argv)
    with open(args.roster, encoding="utf-8") as handle:
        roster = json.load(handle)
    with open(args.vectors, encoding="utf-8") as handle:
        vectors = json.load(handle)
    provider = Provider(
        roster=roster,
        vectors=vectors,
        bearer=_read(args.bearer_file),
        adapter_secret=_read(args.adapter_secret_file),
        authority_bearer=(
            _read(args.authority_bearer_file) if args.authority_bearer_file else None
        ),
        tenant=args.tenant,
        authority_group=args.authority_group,
    )
    servers = serve(
        provider,
        host=args.host,
        lookup_port=args.lookup_port,
        sink_port=args.sink_port,
        tls=server_tls(args.cert, args.key),
    )
    sys.stderr.write(
        f"identity-e2e provider: lookup https :{args.lookup_port}, sink http :{args.sink_port}\n"
    )
    try:
        (wait or threading.Event().wait)()
    finally:
        for server in servers:
            server.shutdown()
    return 0


if __name__ == "__main__":
    sys.exit(main())
