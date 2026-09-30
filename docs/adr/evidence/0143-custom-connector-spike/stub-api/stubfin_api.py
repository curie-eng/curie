"""A stand-in for a third-party finance API that has no hosted MCP server.

It exists so the spike can measure Curie's connector shape against a system
with the two properties that make a custom connector necessary:

* every read needs a bearer access token that expires quickly, and
* the token endpoint ROTATES the refresh token on every exchange and retires
  the previous one, so exactly one process may hold it.

Nothing here is a product. It is a fixture, deliberately stdlib-only so it can
be shipped as a ConfigMap and run on a stock python image.
"""

from __future__ import annotations

import json
import os
import sys
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, urlparse

CLIENT_ID = os.environ.get("STUBFIN_CLIENT_ID", "stubfin-client")
CLIENT_SECRET = os.environ.get("STUBFIN_CLIENT_SECRET", "stubfin-client-secret-placeholder")
ACCESS_TTL = int(os.environ.get("STUBFIN_ACCESS_TTL_SECONDS", "45"))

INVOICES = {
    "2026-Q2": [
        {
            "id": "INV-2026-041",
            "customer": "Northwind Traders",
            "amount": 12500.00,
            "status": "paid",
        },
        {"id": "INV-2026-042", "customer": "Contoso Ltd", "amount": 8200.50, "status": "open"},
        {"id": "INV-2026-043", "customer": "Fabrikam Inc", "amount": 990.00, "status": "overdue"},
    ],
    "2026-Q3": [
        {"id": "INV-2026-057", "customer": "Contoso Ltd", "amount": 4100.00, "status": "open"},
    ],
}


class State:
    def __init__(self) -> None:
        self.lock = threading.Lock()
        self.refresh_seq = int(os.environ.get("STUBFIN_SEED_SEQ", "1"))
        self.current_refresh = f"rt-{self.refresh_seq:04d}"
        self.retired: set[str] = set()
        self.access: dict[str, float] = {}
        self.exchanges = 0
        self.reads = 0
        self.rejected = 0

    def exchange(self, refresh: str) -> dict | None:
        with self.lock:
            if refresh != self.current_refresh:
                self.rejected += 1
                return None
            self.retired.add(self.current_refresh)
            self.refresh_seq += 1
            self.current_refresh = f"rt-{self.refresh_seq:04d}"
            self.exchanges += 1
            token = f"at-{self.exchanges:04d}"
            self.access[token] = time.time() + ACCESS_TTL
            return {
                "token_type": "bearer",
                "access_token": token,
                "expires_in": ACCESS_TTL,
                "refresh_token": self.current_refresh,
            }

    def authorize(self, header: str | None) -> str | None:
        if not header or not header.lower().startswith("bearer "):
            return "missing_token"
        token = header.split(" ", 1)[1].strip()
        with self.lock:
            exp = self.access.get(token)
            if exp is None:
                return "invalid_token"
            if exp < time.time():
                return "expired_token"
            self.reads += 1
        return None


STATE = State()


def log(**fields) -> None:
    fields["ts"] = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
    print(json.dumps(fields), flush=True)


class Handler(BaseHTTPRequestHandler):
    server_version = "stubfin/0.1"

    def log_message(self, *_args) -> None:  # replaced by structured log()
        return

    def _send(self, status: int, body: dict, *, www_auth: str | None = None) -> None:
        data = json.dumps(body).encode()
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(data)))
        if www_auth:
            self.send_header("WWW-Authenticate", www_auth)
        self.end_headers()
        self.wfile.write(data)

    def do_GET(self) -> None:
        url = urlparse(self.path)
        if url.path == "/admin/state":
            with STATE.lock:
                self._send(
                    200,
                    {
                        "current_refresh": STATE.current_refresh,
                        "retired": sorted(STATE.retired),
                        "exchanges": STATE.exchanges,
                        "reads": STATE.reads,
                        "rejected_exchanges": STATE.rejected,
                    },
                )
            return
        if not url.path.startswith("/v1/"):
            self._send(404, {"error": "not_found"})
            return
        why = STATE.authorize(self.headers.get("Authorization"))
        if why:
            log(event="read_refused", path=url.path, reason=why)
            self._send(401, {"error": why}, www_auth=f'Bearer error="{why}"')
            return
        if url.path == "/v1/invoices":
            period = parse_qs(url.query).get("period", [""])[0]
            rows = INVOICES.get(period)
            if rows is None:
                log(event="read", path=url.path, period=period, status=404)
                self._send(404, {"error": "unknown_period", "period": period})
                return
            log(event="read", path=url.path, period=period, status=200, rows=len(rows))
            self._send(200, {"period": period, "invoices": rows})
            return
        if url.path.startswith("/v1/invoices/"):
            wanted = url.path.rsplit("/", 1)[1]
            for rows in INVOICES.values():
                for row in rows:
                    if row["id"] == wanted:
                        log(event="read", path=url.path, status=200)
                        self._send(200, row)
                        return
            log(event="read", path=url.path, status=404)
            self._send(404, {"error": "unknown_invoice", "id": wanted})
            return
        self._send(404, {"error": "not_found"})

    def do_POST(self) -> None:
        url = urlparse(self.path)
        if url.path != "/oauth/token":
            self._send(404, {"error": "not_found"})
            return
        length = int(self.headers.get("Content-Length") or 0)
        raw = self.rfile.read(length).decode()
        ctype = self.headers.get("Content-Type", "")
        if "json" in ctype:
            form = json.loads(raw or "{}")
        else:
            form = {k: v[0] for k, v in parse_qs(raw).items()}
        if form.get("client_id") != CLIENT_ID or form.get("client_secret") != CLIENT_SECRET:
            log(event="token_refused", reason="invalid_client")
            self._send(401, {"error": "invalid_client"})
            return
        if form.get("grant_type") != "refresh_token":
            self._send(400, {"error": "unsupported_grant_type"})
            return
        issued = STATE.exchange(form.get("refresh_token", ""))
        if issued is None:
            log(
                event="token_refused",
                reason="invalid_grant",
                presented=form.get("refresh_token", ""),
                current=STATE.current_refresh,
            )
            self._send(
                400,
                {"error": "invalid_grant", "error_description": "refresh token retired or unknown"},
            )
            return
        log(
            event="token_issued",
            access=issued["access_token"],
            rotated_to=issued["refresh_token"],
            expires_in=ACCESS_TTL,
        )
        self._send(200, issued)


def main() -> int:
    port = int(os.environ.get("PORT", "8080"))
    log(event="startup", port=port, seed_refresh=STATE.current_refresh, access_ttl=ACCESS_TTL)
    ThreadingHTTPServer(("0.0.0.0", port), Handler).serve_forever()
    return 0


if __name__ == "__main__":
    sys.exit(main())
