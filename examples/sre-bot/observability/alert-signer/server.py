"""Sign Alertmanager notifications for Curie hook ingress (#2572).

Alertmanager cannot HMAC the body. This adapter is the one supported source
binding: it injects a partition-safe ``curie_partition`` derived from groupKey,
assigns a stable delivery id, stamps the current unix time, and signs the
timestamp, delivery id, decoded hook name, requested tool policy and raw body.
The context is compact ASCII JSON for ``[hook, tool_access]``, with ``null``
when the policy is omitted. Its length frames the boundary before the body.
The signed bytes include a fixed version prefix:
``b"curie.hook.delivery.v2\\n" +
f"{timestamp}.{delivery}.{len(context)}:".encode() + context + body``.
The hook and policy come from ``CURIE_HOOK_URL``, which is also the destination
for the POST. Duplicate or invalid ``tool_access`` query values are refused.
The request sends ``X-Curie-Timestamp`` and ``X-Curie-Delivery-Id`` alongside
the signature. Curie refuses a timestamp more than five minutes from its clock,
so the forwarder must be roughly in sync.
The delivery id must not contain "." (the signed-material delimiter); the hex ids
this adapter assigns never do.

A protected source (``CURIE_HOOK_SOURCE_MODE=protected``, @spec
HOOK-SOURCE-POLICY-6) requires ``tool_access=read-only`` in the URL and signs
with the hook's scoped source key, never the shared agent secret. Before each
delivery it posts the signed support probe to ``<hook path>/support``, which
creates no claim, queue entry, message or turn, and forwards only when the API
answers 200 ``supported``. Each success receipt must report ``read-only`` as its
requested, effective and alias policy and name the source generation the probe
reported. Any other
answer is refused with HTTP 502 to the source and one warning naming only a
fixed reason. The default ``ordinary`` mode is unchanged.

Environment:
  CURIE_HOOK_URL           Full ingest URL, including hook and optional tool_access
  CURIE_HOOK_SECRET        The derived per-agent hook secret (ordinary mode only)
  CURIE_HOOK_SOURCE_MODE   ``ordinary`` (default) or ``protected``
  CURIE_HOOK_SOURCE_SECRET The hook's scoped source key (protected mode only)
  LISTEN_ADDR              Bind address, default 0.0.0.0:8080
"""

from __future__ import annotations

import hashlib
import hmac
import http.client
import json
import logging
import os
import time
import urllib.error
import urllib.parse
import urllib.request
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any

logger = logging.getLogger(__name__)

PROTECTED = "protected"
READ_ONLY = "read-only"
# Exact signed bytes of the support probe body (HookSupportIn).
SUPPORT_BODY = b'{"tool_access":"read-only"}'
# The API's closed HookSupportReason enum; anything else is malformed.
SUPPORT_REASONS = frozenset(
    {
        "supported",
        "source_unconfigured",
        "source_closed",
        "runtime_unavailable",
        "qualification_unavailable",
        "evidence_missing",
        "evidence_expired",
        "broker_unavailable",
        "broker_identity_mismatch",
        "configuration_unsupported",
    }
)
_ANSWER_LIMIT = 65536


class ProtectedRefusal(Exception):
    """A protected delivery the signer refuses; ``reason`` is a fixed safe token."""

    def __init__(self, reason: str) -> None:
        super().__init__(reason)
        self.reason = reason


def partition_value(group_key: str) -> str:
    return hashlib.sha256(group_key.encode()).hexdigest()[:32]


def delivery_id(group_key: str, status: str, fingerprints: list[str], starts_at: list[str]) -> str:
    material = "|".join([group_key, status, *fingerprints, *starts_at])
    return hashlib.sha256(material.encode()).hexdigest()


def _material(
    timestamp: str, delivery: str, body: bytes, *, hook: str, tool_access: str | None
) -> bytes:
    context = json.dumps([hook, tool_access], ensure_ascii=True, separators=(",", ":")).encode(
        "ascii"
    )
    return (
        b"curie.hook.delivery.v2\n"
        + f"{timestamp}.{delivery}.{len(context)}:".encode()
        + context
        + body
    )


def sign(
    secret: str,
    timestamp: str,
    delivery: str,
    body: bytes,
    *,
    hook: str,
    tool_access: str | None,
) -> str:
    material = _material(timestamp, delivery, body, hook=hook, tool_access=tool_access)
    return "sha256=" + hmac.new(secret.encode(), material, hashlib.sha256).hexdigest()


def sign_support(
    secret: str,
    timestamp: str,
    delivery: str,
    body: bytes,
    *,
    hook: str,
    tool_access: str | None,
) -> str:
    """The support probe signature: a purpose label before the delivery bytes.

    A support signature cannot enqueue at ingress and a delivery signature
    cannot authenticate the probe. @spec PROTECTED-HOOK-SOURCE-9.
    """
    material = b"curie.hook.support.v1\n" + _material(
        timestamp, delivery, body, hook=hook, tool_access=tool_access
    )
    return "sha256=" + hmac.new(secret.encode(), material, hashlib.sha256).hexdigest()


def source_mode() -> str:
    """The configured source mode; a protected mode never falls back to ordinary."""

    mode = os.environ.get("CURIE_HOOK_SOURCE_MODE") or "ordinary"
    if mode not in ("ordinary", PROTECTED):
        raise ValueError("CURIE_HOOK_SOURCE_MODE must be ordinary or protected")
    return mode


def target() -> tuple[str, str | None, str]:
    """Return ``(hook, tool_access, secret)`` for the configured mode.

    @spec HOOK-SOURCE-POLICY-6: a protected source requests ``read-only`` and
    holds only its scoped key, so the shared agent secret must be absent.
    """

    hook_url = urllib.parse.urlsplit(os.environ["CURIE_HOOK_URL"])
    hook = urllib.parse.unquote(hook_url.path.rsplit("/", 1)[-1])
    policies = urllib.parse.parse_qs(hook_url.query, keep_blank_values=True).get("tool_access", [])
    if len(policies) > 1 or (policies and policies[0] != READ_ONLY):
        raise ValueError("tool_access must be omitted or supplied once as read-only")
    tool_access = policies[0] if policies else None
    if source_mode() != PROTECTED:
        if os.environ.get("CURIE_HOOK_SOURCE_SECRET"):
            raise ValueError("a scoped source key requires CURIE_HOOK_SOURCE_MODE=protected")
        return hook, tool_access, os.environ["CURIE_HOOK_SECRET"]
    if tool_access != READ_ONLY:
        raise ValueError("a protected source must request tool_access=read-only")
    if os.environ.get("CURIE_HOOK_SECRET"):
        raise ValueError("a protected source must not hold the shared agent secret")
    secret = os.environ.get("CURIE_HOOK_SOURCE_SECRET") or ""
    if not secret:
        raise ValueError("a protected source requires CURIE_HOOK_SOURCE_SECRET")
    return hook, tool_access, secret


def prepare(payload: dict[str, Any]) -> tuple[bytes, str, str, str]:
    """Return ``(body, signature, delivery, timestamp)`` for one forward."""

    hook, tool_access, secret = target()
    group_key = str(payload.get("groupKey") or "")
    status = str(payload.get("status") or "")
    alerts = [alert for alert in payload.get("alerts") or [] if isinstance(alert, dict)]
    fingerprints = sorted(str(alert.get("fingerprint") or "") for alert in alerts)
    starts_at = sorted(str(alert.get("startsAt") or "") for alert in alerts)
    forwarded = dict(payload)
    forwarded["curie_partition"] = partition_value(group_key)
    body = json.dumps(forwarded, separators=(",", ":")).encode()
    delivery = delivery_id(group_key, status, fingerprints, starts_at)
    timestamp = str(int(time.time()))
    signature = sign(
        secret,
        timestamp,
        delivery,
        body,
        hook=hook,
        tool_access=tool_access,
    )
    return body, signature, delivery, timestamp


def forward(body: bytes, signature: str, delivery: str, timestamp: str) -> int:
    request = urllib.request.Request(
        os.environ["CURIE_HOOK_URL"],
        data=body,
        method="POST",
        headers={
            "Content-Type": "application/json",
            "X-Curie-Signature-256": signature,
            "X-Curie-Delivery-Id": delivery,
            "X-Curie-Timestamp": timestamp,
        },
    )
    try:
        with urllib.request.urlopen(request, timeout=15) as response:
            return int(response.status)
    except urllib.error.HTTPError as exc:
        return int(exc.code)


def _exchange(url: str, body: bytes, headers: dict[str, str]) -> tuple[int, Any]:
    """POST and return the status with the JSON object answer, or None."""

    request = urllib.request.Request(
        url, data=body, method="POST", headers={"Content-Type": "application/json", **headers}
    )
    try:
        with urllib.request.urlopen(request, timeout=15) as response:
            status, raw = int(response.status), response.read(_ANSWER_LIMIT)
    except urllib.error.HTTPError as exc:
        status, raw = int(exc.code), exc.read(_ANSWER_LIMIT)
    try:
        answer = json.loads(raw.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError):
        answer = None
    return status, answer if isinstance(answer, dict) else None


def check_support(hook: str, secret: str, delivery: str) -> str:
    """Refuse unless the API reports supported protected admission for this hook.

    Returns the source generation the probe reported.

    The probe is observational: no hook claim, queue entry, Slack post or turn.
    A shared or revoked key answers 401 and an unconfigured hook 503; neither
    is supported. An OpenAPI schema check cannot stand in for this answer.
    @spec HOOK-SOURCE-POLICY-6 @spec PROTECTED-HOOK-SOURCE-9.
    """

    hook_url = urllib.parse.urlsplit(os.environ["CURIE_HOOK_URL"])
    url = urllib.parse.urlunsplit(
        (hook_url.scheme, hook_url.netloc, hook_url.path + "/support", "", "")
    )
    timestamp = str(int(time.time()))
    signature = sign_support(
        secret, timestamp, delivery, SUPPORT_BODY, hook=hook, tool_access=READ_ONLY
    )
    status, answer = _exchange(
        url,
        SUPPORT_BODY,
        {
            "X-Curie-Signature-256": signature,
            "X-Curie-Delivery-Id": delivery,
            "X-Curie-Timestamp": timestamp,
        },
    )
    if status == 401:
        raise ProtectedRefusal("support_unauthenticated")
    if status not in (200, 503) or answer is None:
        raise ProtectedRefusal("support_unavailable")
    reason = answer.get("reason")
    if reason not in SUPPORT_REASONS:
        raise ProtectedRefusal("support_malformed")
    if reason != "supported":
        raise ProtectedRefusal(f"support_{reason}")
    runtime = [answer.get(key) for key in ("runtime_id", "runtime_generation", "qualification_id")]
    if (
        status != 200
        or answer.get("supported") is not True
        or answer.get("requested_tool_access") != READ_ONLY
        or answer.get("effective_tool_access") != READ_ONLY
        or not all(isinstance(member, str) and member for member in runtime)
        or not isinstance(answer.get("source_generation"), str)
        or not answer["source_generation"]
    ):
        raise ProtectedRefusal("support_malformed")
    return str(answer["source_generation"])


def verify_receipt(receipt: Any, generation: str) -> None:
    """Refuse a success receipt that does not attest protected read-only admission.

    Its source generation must be the one the support probe reported.

    @spec HOOK-SOURCE-POLICY-6 @spec PROTECTED-HOOK-SOURCE-8.
    """

    if receipt is None:
        raise ProtectedRefusal("receipt_malformed")
    for field in ("requested_tool_access", "effective_tool_access", "tool_access"):
        if receipt.get(field) != READ_ONLY:
            raise ProtectedRefusal("receipt_not_read_only")
    if receipt.get("source_generation") != generation:
        raise ProtectedRefusal("receipt_unbound_source")
    if receipt.get("acceptance_status") not in ("accepted", "preparing"):
        raise ProtectedRefusal("receipt_not_admitted")


def deliver(body: bytes, signature: str, delivery: str, timestamp: str) -> int:
    """Forward one prepared delivery; a protected source checks support and receipt."""

    if source_mode() != PROTECTED:
        return forward(body, signature, delivery, timestamp)
    hook, _tool_access, secret = target()
    generation = check_support(hook, secret, delivery)
    status, receipt = _exchange(
        os.environ["CURIE_HOOK_URL"],
        body,
        {
            "X-Curie-Signature-256": signature,
            "X-Curie-Delivery-Id": delivery,
            "X-Curie-Timestamp": timestamp,
        },
    )
    if 200 <= status < 300:
        verify_receipt(receipt, generation)
    return status


class Handler(BaseHTTPRequestHandler):
    def log_message(self, format: str, *args: Any) -> None:
        return

    def do_POST(self) -> None:  # noqa: N802
        expected = os.environ.get("CURIE_SIGNER_TOKEN") or ""
        presented = self.headers.get("Authorization") or ""
        want = f"Bearer {expected}"
        if not expected or not hmac.compare_digest(presented.encode(), want.encode()):
            self.send_response(401)
            self.end_headers()
            return
        length = int(self.headers.get("Content-Length") or "0")
        raw = self.rfile.read(length)
        try:
            payload = json.loads(raw.decode("utf-8"))
            if not isinstance(payload, dict):
                raise ValueError("payload is not an object")
            body, signature, delivery, timestamp = prepare(payload)
        except (UnicodeDecodeError, json.JSONDecodeError, ValueError, KeyError):
            self.send_response(400)
            self.end_headers()
            return
        # @spec SIGNER-DIAGNOSTICS-1: exception text may contain credentials.
        try:
            status = deliver(body, signature, delivery, timestamp)
        except ProtectedRefusal as refusal:
            # @spec HOOK-SOURCE-POLICY-6: only the fixed reason, never a credential.
            logger.warning("protected hook delivery refused (reason=%s)", refusal.reason)
            self.send_response(502)
            self.end_headers()
            return
        except (urllib.error.URLError, OSError, http.client.HTTPException):
            logger.warning("hook ingress forwarding failed (reason=transport_error)")
            self.send_response(502)
            self.end_headers()
            return
        if not 200 <= status < 300:
            logger.warning("hook ingress rejected delivery (status=%d)", status)
        self.send_response(200 if 200 <= status < 300 else 502)
        self.end_headers()


def main() -> None:
    # Refuse to start on an incomplete source configuration, so a missing or
    # mixed credential fails the rollout instead of every forward.
    target()
    addr = os.environ.get("LISTEN_ADDR", "0.0.0.0:8080")
    host, port_s = addr.rsplit(":", 1)
    server = ThreadingHTTPServer((host, int(port_s)), Handler)
    server.serve_forever()


if __name__ == "__main__":
    main()
