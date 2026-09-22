"""The smallest custom connector Curie can host today, measured.

One MCP server, HTTP transport, two read tools, for a third-party finance API
that has no hosted MCP server of its own. It exists to answer, on a real
cluster and with nothing but a released `curie` binary, the questions the ADR
beside it asks: how a connector is written, how it holds and rotates a provider
credential, how it is built and pinned, and what it logs.

SHAPE
=====

* Language and framework: Python plus the official `mcp` SDK (`MCPServer`).
  The same pair the platform's own reference bundle uses, so the platform's
  known traps (annotation introspection, transport wiring) are the same ones.
* Transport: streamable HTTP on `0.0.0.0:<PORT>` at `/mcp`. A hosted connector
  is a Deployment behind a Service; stdio ends at end-of-input and exits 0,
  which reads as success in a deploy log.
* Tool annotations: every tool sets `readOnlyHint`. The runner classifies the
  WHOLE surface as potentially write-capable if any tool omits it.

CREDENTIAL
==========

Held here, by reference: the client id, client secret and refresh token arrive
as environment variables from a Kubernetes Secret the operator provisioned,
named in `connectors.yaml` under `secrets:` with `from_secret`. The provider
rotates the refresh token on every exchange and retires the previous one, so:

* exactly one process holds it (`replicas: 1` is rendered by the platform and
  the operator retires any older holder as part of deploying this), and
* a reissued token is written back to the Secret BEFORE the access token that
  came with it is used. A restart otherwise begins from a retired token and
  needs a human re-authorization.

The write-back is a PATCH of one Secret through the Kubernetes API with the
pod's own service account. `connectors.yaml` cannot declare a ServiceAccount,
so the Role in `manifests/` binds to `default`, which is wider than one pod.

LOGGING
=======

One JSON line per tool call and per token event on stdout, so `kubectl logs`
answers "did anyone use it and what failed" without a tracing stack.
"""

# No `from __future__ import annotations`: the SDK introspects tool signatures
# and stringized annotations break that at import time.

import base64
import json
import logging
import os
import ssl
import sys
import tempfile
import threading
import time
import urllib.error
import urllib.request
from typing import Any

import httpx
from mcp.server.mcpserver import MCPServer
from mcp.server.mcpserver.exceptions import ToolError
from mcp.types import ToolAnnotations

log = logging.getLogger("stubfin-connector")

API_BASE = os.environ.get("FIN_API_BASE", "").rstrip("/")
CLIENT_ID = os.environ.get("FIN_CLIENT_ID", "")
CLIENT_SECRET = os.environ.get("FIN_CLIENT_SECRET", "")
SEED_REFRESH = os.environ.get("FIN_REFRESH_TOKEN", "")
CREDENTIAL_STORE = os.environ.get("FIN_CREDENTIAL_STORE", "stubfin-credentials")
CREDENTIAL_STORE_KEY = os.environ.get("FIN_CREDENTIAL_STORE_KEY", "FIN_REFRESH_TOKEN")
TIMEOUT = float(os.environ.get("FIN_TIMEOUT_SECONDS", "20"))
SA = "/var/run/secrets/kubernetes.io/serviceaccount"

READ = ToolAnnotations(
    readOnlyHint=True,
    destructiveHint=False,
    idempotentHint=True,
    openWorldHint=True,
)


def emit(**fields: Any) -> None:
    """One structured line per event. stdout, flushed, no secrets."""

    fields["ts"] = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
    print(json.dumps(fields), flush=True)


# --------------------------------------------------------------------------
# Credential holder: one per process, refresh under a lock, persist first.
# --------------------------------------------------------------------------


class TokenPersistError(RuntimeError):
    """The reissued refresh token could not be stored. Never swallowed."""


def _service_account() -> tuple[str, str, ssl.SSLContext] | None:
    if not os.path.isdir(SA):
        return None
    with open(f"{SA}/token", encoding="utf-8") as fh:
        token = fh.read().strip()
    with open(f"{SA}/namespace", encoding="utf-8") as fh:
        namespace = fh.read().strip()
    return token, namespace, ssl.create_default_context(cafile=f"{SA}/ca.crt")


def persist_refresh_token(new_token: str) -> None:
    """PATCH the one Secret this connector may touch. Retries, then raises."""

    sa = _service_account()
    if sa is None:
        raise TokenPersistError("no service account mounted; nowhere to store a reissued token")
    sa_token, namespace, ctx = sa
    body = json.dumps(
        {"data": {CREDENTIAL_STORE_KEY: base64.b64encode(new_token.encode()).decode()}}
    ).encode()
    last = ""
    for attempt in range(1, 6):
        request = urllib.request.Request(
            f"https://kubernetes.default.svc/api/v1/namespaces/{namespace}/secrets/{CREDENTIAL_STORE}",
            data=body,
            method="PATCH",
            headers={
                "Authorization": f"Bearer {sa_token}",
                "Content-Type": "application/merge-patch+json",
                "Accept": "application/json",
            },
        )
        try:
            with urllib.request.urlopen(request, context=ctx, timeout=30) as reply:
                if reply.status in (200, 201):
                    emit(
                        event="token_persisted",
                        store=CREDENTIAL_STORE,
                        key=CREDENTIAL_STORE_KEY,
                        attempt=attempt,
                    )
                    return
                last = f"status {reply.status}"
        except urllib.error.HTTPError as error:
            last = f"{error.code} {error.read()[:200].decode(errors='replace')}"
            if error.code in (401, 403, 404):
                break  # a wrong Role, ServiceAccount or Secret name; retrying cannot fix it
        except OSError as error:
            last = str(error)
        time.sleep(2 * attempt)
    raise TokenPersistError(
        f"could not store the reissued refresh token in Secret {CREDENTIAL_STORE}: {last}. "
        "The provider has already retired the previous token, so the new one exists only in this "
        "process. Do not restart this connector until the Role or Secret is fixed."
    )


class TokenHolder:
    def __init__(self, refresh: str) -> None:
        self._lock = threading.Lock()
        self._refresh = refresh
        self._access = ""
        self._expires_at = 0.0

    def bearer(self) -> str:
        with self._lock:
            if self._access and time.time() < self._expires_at - 5:
                return self._access
            return self._exchange()

    def _exchange(self) -> str:
        started = time.monotonic()
        try:
            reply = httpx.post(
                f"{API_BASE}/oauth/token",
                data={
                    "grant_type": "refresh_token",
                    "refresh_token": self._refresh,
                    "client_id": CLIENT_ID,
                    "client_secret": CLIENT_SECRET,
                },
                timeout=TIMEOUT,
            )
        except httpx.HTTPError as error:
            emit(event="token_refresh", ok=False, error=f"transport: {error}")
            raise ToolError(f"the finance API's token endpoint was unreachable: {error}") from None
        duration = int((time.monotonic() - started) * 1000)
        if reply.status_code != 200:
            emit(
                event="token_refresh",
                ok=False,
                upstream_status=reply.status_code,
                duration_ms=duration,
                body=reply.text[:200],
            )
            raise ToolError(
                f"the finance API refused the refresh token "
                f"({reply.status_code}: {reply.text[:120]}). If it says invalid_grant, "
                "another holder has rotated it and a human must re-authorize."
            )
        issued = reply.json()
        new_refresh = issued.get("refresh_token")
        rotated = bool(new_refresh) and new_refresh != self._refresh
        if rotated:
            # Persist BEFORE adopting: if the store is unreachable the process
            # keeps the old access token behaviour and the operator is told.
            try:
                persist_refresh_token(new_refresh)
            except TokenPersistError as error:
                emit(
                    event="token_refresh", ok=False, rotated=True, persisted=False, error=str(error)
                )
                self._refresh = new_refresh  # the only copy now; keep it in memory
                raise ToolError(str(error)) from None
            self._refresh = new_refresh
        self._access = issued["access_token"]
        self._expires_at = time.time() + float(issued.get("expires_in", 60))
        emit(
            event="token_refresh",
            ok=True,
            upstream_status=200,
            duration_ms=duration,
            rotated=rotated,
            expires_in=issued.get("expires_in"),
        )
        return self._access


_HOLDER: TokenHolder | None = None


def holder() -> TokenHolder:
    global _HOLDER
    if _HOLDER is None:
        missing = [
            n
            for n, v in (
                ("FIN_API_BASE", API_BASE),
                ("FIN_CLIENT_ID", CLIENT_ID),
                ("FIN_CLIENT_SECRET", CLIENT_SECRET),
                ("FIN_REFRESH_TOKEN", SEED_REFRESH),
            )
            if not v
        ]
        if missing:
            raise ToolError(
                f"the connector is missing {', '.join(missing)}; "
                "they come from the Secret named in connectors.yaml"
            )
        _HOLDER = TokenHolder(SEED_REFRESH)
    return _HOLDER


# --------------------------------------------------------------------------
# Tools
# --------------------------------------------------------------------------


def _get(tool: str, path: str, params: dict[str, str] | None, **fields: Any) -> dict[str, Any]:
    started = time.monotonic()
    try:
        reply = httpx.get(
            f"{API_BASE}{path}",
            params=params,
            headers={"Authorization": f"Bearer {holder().bearer()}"},
            timeout=TIMEOUT,
        )
    except httpx.HTTPError as error:
        emit(event="tool_call", tool=tool, ok=False, error=f"transport: {error}", **fields)
        raise ToolError(f"the finance API was unreachable: {error}") from None
    duration = int((time.monotonic() - started) * 1000)
    ok = reply.status_code == 200
    emit(
        event="tool_call",
        tool=tool,
        ok=ok,
        upstream_status=reply.status_code,
        duration_ms=duration,
        **fields,
    )
    if not ok:
        raise ToolError(f"the finance API answered {reply.status_code}: {reply.text[:200]}")
    return reply.json()


mcp = MCPServer("stubfin")


@mcp.tool(annotations=READ)
def list_invoices(period: str) -> str:
    """List the invoices the finance system holds for a fiscal period such as 2026-Q2.

    Returns JSON: the period and a list of invoices with id, customer, amount and
    status. Figures come from the finance system; nothing here is computed.
    """

    return json.dumps(_get("list_invoices", "/v1/invoices", {"period": period}, period=period))


@mcp.tool(annotations=READ)
def invoice(invoice_id: str) -> str:
    """One invoice by id, such as INV-2026-042, as the finance system reports it."""

    return json.dumps(_get("invoice", f"/v1/invoices/{invoice_id}", None, invoice_id=invoice_id))


def _tmp_writable() -> bool:
    """Whether the process has a usable temp dir. Measured on the cluster: no.

    Under the rendered securityContext (`readOnlyRootFilesystem`, no emptyDir)
    `tempfile.gettempdir()` itself raises FileNotFoundError, so even asking
    the question has to be guarded; the first build of this file crashed at
    startup on exactly that call.
    """

    try:
        fd, path = tempfile.mkstemp()
        os.close(fd)
        os.unlink(path)
        return True
    except OSError:
        return False


def _tmpdir() -> str | None:
    try:
        return tempfile.gettempdir()
    except OSError:
        return None


def main() -> int:
    emit(
        event="startup",
        api_base=API_BASE or None,
        credential_present=bool(SEED_REFRESH),
        tmp_writable=_tmp_writable(),
        tmpdir=_tmpdir(),
        argv=sys.argv[1:],
    )
    mcp.run(
        transport="streamable-http",
        host=os.environ.get("BIND_ADDRESS", "0.0.0.0"),
        port=int(os.environ.get("PORT", "8000")),
        streamable_http_path="/mcp",
    )
    return 0


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    sys.exit(main())
