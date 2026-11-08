#!/usr/bin/env python3
"""The reference human-restricted hosted connector (ADR 0270 decision 5, ADR 0271 decision 7).

A small MCP server over streamable HTTP (``POST /mcp``, one JSON answer per
request) with three tools that share one effect ledger. ``record_effect``
needs a requester and current authority. ``execute_attested`` is the ordinary
approved execution tool: it also needs the caller proxy's attestation that it
verified and spent an approval grant for exactly this call.
``execute_approved`` is the ADR 0271 per-request approval tool: it needs the
attestation and also a valid approval ticket in its ``approval_ticket``
argument. The two protected tools refuse a call without an attestation whether
or not the proxy's gated list names them, so their protection never rests on
that list.

Before every effect a tool runs its checks in this order, and the first one
that fails refuses the call:

1. Attestation, for the protected tools only. The caller proxy sets exactly
   one ``X-Curie-Grant-Verified`` header, the spent grant's canonical lowercase
   UUID ``jti``, on its loopback forward, and only after it verified and spent
   a grant for this call. The header is trusted only from that loopback peer,
   as the principal is, so a caller on the connector's ``-direct`` Service
   cannot forge it. No header, or any other peer, refuses
   ``attestation_missing``. A repeated header, or a value that is not a
   canonical lowercase UUID, refuses ``attestation_invalid``. A grant id that
   already applied an effect, on either protected tool, refuses
   ``grant_replayed``. Only an applied effect remembers a grant id, so a
   refused call never burns one; stopping a resend is the proxy's spend.
2. Requester. The requester comes only from the single ``X-Curie-Principal``
   header the caller proxy sets on its loopback forward (ADR 0270 decision 4).
   A request from any other peer never establishes a requester, whatever it
   sends. No requester refuses ``missing_requester``; a repeated or unreadable
   header refuses ``requester_invalid``. Tool arguments are never read for
   identity, so a model-written name cannot grant or redirect authority.
3. Ticket, for ``execute_approved`` only. A non-string or absent
   ``approval_ticket`` refuses ``ticket_missing``, and a connector without all
   four ticket settings refuses ``ticket_unconfigured``. The ticket must be
   ``cat1.<payload>.<signature>`` in canonical unpadded base64url with a valid
   Ed25519 signature (RFC 8032, verified here in plain Python) by the
   configured public key over ``cat1.<payload>``. The payload must be strict
   JSON with no duplicate key, NaN or Infinity, in the closed shape, types,
   bounds and text hygiene of ``curie_internal.approver_ticket.ApprovalTicket``,
   and its bytes must be their own canonical re-encoding. Anything else
   refuses ``ticket_invalid``. The signed key id, connector, tool
   (``execute_approved``), route and agent id must be this connector's
   configuration, or ``ticket_scope_mismatch``. The signed requester must equal
   the proxy principal exactly, or ``ticket_requester_mismatch``. The signed
   ``arguments_sha256`` must be the SHA-256 of the call's arguments without the
   top-level ``approval_ticket``, in the production canonical-arguments
   spelling, or ``ticket_arguments_mismatch``. No clock is consulted: an
   approval answered after its five-minute preparation window still executes
   (ADR 0271 decision 7).
4. Authority. ``POST <AUTHORITY_URL>/authority`` with ``Authorization: Bearer
   <AUTHORITY_BEARER>`` and the principal as the JSON body, under one total
   timeout. Only a 200 whose body is exactly ``{"decision": "allow"}`` or
   ``{"decision": "deny"}`` is a decision. Deny refuses ``not_authorized``;
   anything else refuses ``authority_unavailable``. No earlier answer is cached.

A refusal is a tool result with ``isError`` true and the text
``refused <code>``. An effect appends to the in-memory ledger and answers
``effect=<n> requester=<fingerprint>``, which a protected tool follows with
``grant=<fingerprint>``. A fingerprint is the first 16 hex digits of the
SHA-256 of the header value. A protected call whose ``drop_reply`` argument is
true applies its effect and then closes the connection without any answer,
which is the uncertain-outcome probe; a refusal is still answered. Every
decoded value lives only in the handling request's locals, so no process
global, connection, MCP session or earlier request can supply a principal,
attestation or ticket to another call. The only shared state is the ledger and
the remembered grant ids, both behind one lock. Each call writes one stderr
line, ``<tool> outcome=<ok|code> requester=<fingerprint or -> grant=<fingerprint
or ->``, and nothing else about the requester, the grant or the ticket is
logged. A refusal at the attestation step logs neither fingerprint, and
``record_effect`` reads no attestation, so its line always ends ``grant=-``.

Standard library only, Python 3.12. The identity E2E harness ships this file's
text inline as a connectors.yaml env value (``identity_assets.reference_bundle``)
and the renderer substitutes dollar-brace placeholders in env values, so this
file must never contain one. Configured from the environment:
``AUTHORITY_URL``, ``AUTHORITY_BEARER`` and ``PORT`` (default 8000), and for
``execute_approved`` the four ticket settings ``TICKET_PUBLIC_KEY`` (the
32-byte Ed25519 public key in standard base64), ``TICKET_KEY_ID``,
``TICKET_ROUTE`` and ``TICKET_AGENT_ID``. The ticket settings are optional:
while any of them is unset or blank the connector serves and
``execute_approved`` refuses ``ticket_unconfigured``. ``TICKET_CONNECTOR``
names the connector a ticket must be signed for and defaults to
``human-restricted``.
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
GRANT_VERIFIED_HEADER = "X-Curie-Grant-Verified"
CONNECTOR_NAME = "human-restricted"
TOOL_NAME = "record_effect"
ATTESTED_TOOL = "execute_attested"
APPROVED_TOOL = "execute_approved"
TOOL_NAMES = (TOOL_NAME, ATTESTED_TOOL, APPROVED_TOOL)
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
ATTESTATION_MISSING = "attestation_missing"
ATTESTATION_INVALID = "attestation_invalid"
GRANT_REPLAYED = "grant_replayed"
TICKET_MISSING = "ticket_missing"
TICKET_INVALID = "ticket_invalid"
TICKET_SCOPE_MISMATCH = "ticket_scope_mismatch"
TICKET_REQUESTER_MISMATCH = "ticket_requester_mismatch"
TICKET_ARGUMENTS_MISMATCH = "ticket_arguments_mismatch"
TICKET_UNCONFIGURED = "ticket_unconfigured"

# The header reader's bounds, the same as the caller proxy's (ADR 0270 decision 4).
MAX_HEADER_CHARS = 32 * 1024
ISSUER_MAX_CHARS = 2048
SUBJECT_MAX_CHARS = 256
_PRINCIPAL_FIELDS = frozenset({"tenant_id", "issuer", "subject"})
_URL_SAFE = re.compile(r"[A-Za-z0-9_-]+")
_UUID = re.compile(r"[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}")
_CONTENT_LENGTH = re.compile(r"[0-9]{1,9}")

# The ticket reader's bounds, the same as curie_internal.approver_ticket's (ADR 0271
# decision 3 and INTERFACE.md "Per-request approval tickets").
MAX_TICKET_CHARS = 16_384
MAX_TIMESTAMP = 2**63 - 1
MAX_PREPARATION_S = 300
MAX_APPROVERS = 16
MAX_RECIPIENT_ACCOUNTS = 16
SHORT_TEXT_MAX_BYTES = 256
IDENTIFIER_MAX_CHARS = 256
AUTHORITY_MAX_CHARS = 2048
NONCE_BYTES = 32
TICKET_PREFIX = "cat1"
_DIGEST = re.compile(r"[0-9a-f]{64}")
_TICKET_FIELDS = frozenset(
    {
        "version",
        "purpose",
        "tenant_id",
        "agent_id",
        "connector",
        "tool",
        "route",
        "key_id",
        "requester",
        "nonce",
        "issued_at",
        "expires_at",
        "arguments_sha256",
        "approvers",
    }
)
_SCOPE_FIELDS = ("key_id", "connector", "tool", "route", "agent_id")
_APPROVER_FIELDS = _PRINCIPAL_FIELDS | {"recipient_accounts"}
_ACCOUNT_FIELDS = frozenset({"provider", "installation_id", "namespace", "account_id"})
_NAMESPACE_FIELDS = frozenset({"id", "authority", "kind", "key"})

MAX_REQUEST_BYTES = 1024 * 1024
MAX_AUTHORITY_BYTES = 4096
# Echoed when the client asks for one of these; otherwise the first is offered.
PROTOCOL_VERSIONS = ("2025-11-25", "2025-06-18", "2025-03-26", "2024-11-05")

PARSE_ERROR = -32700
INVALID_REQUEST = -32600
METHOD_NOT_FOUND = -32601
INVALID_PARAMS = -32602

_NOTE: dict[str, Any] = {"type": "string", "description": "A free-text note."}
_DROP_REPLY: dict[str, Any] = {
    "type": "boolean",
    "description": (
        "Apply the effect, then close the connection without an answer "
        "(the uncertain-outcome probe)."
    ),
}

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
TOOLS: list[dict[str, Any]] = [
    TOOL,
    {
        "name": ATTESTED_TOOL,
        "description": (
            "Record one synthetic effect as the verified requester, only on a call the "
            "caller proxy attests it verified and spent an approval grant for."
        ),
        "inputSchema": {
            "type": "object",
            "properties": {"note": _NOTE, "drop_reply": _DROP_REPLY},
        },
    },
    {
        "name": APPROVED_TOOL,
        "description": (
            "Record one synthetic effect as the verified requester, only on a call the "
            "caller proxy attests and whose arguments carry the approval ticket the "
            "request was approved with."
        ),
        "inputSchema": {
            "type": "object",
            "properties": {
                "note": _NOTE,
                "drop_reply": _DROP_REPLY,
                "approval_ticket": {
                    "type": "string",
                    "description": "The signed ADR 0271 approval ticket.",
                },
            },
        },
    },
]


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


def fingerprint(value: str) -> str:
    """What the ledger and the log carry instead of a principal header or a grant id."""

    return hashlib.sha256(value.encode("ascii")).hexdigest()[:16]


# --- Ed25519 verification (RFC 8032 section 5.1) -------------------------------------
#
# Points are extended homogeneous coordinates (X, Y, Z, T) with x = X/Z, y = Y/Z
# and x * y = T/Z (section 5.1.4). Speed does not matter here: a call verifies
# one signature.

_P = 2**255 - 19  # section 5.1: the field prime p
_L = 2**252 + 27742317777372353535851937790883648493  # section 5.1: the group order L
_D = -121665 * pow(121666, -1, _P) % _P  # section 5.1: the curve constant d
_SQRT_M1 = pow(2, (_P - 1) // 4, _P)  # section 5.1.3 step 3: a square root of -1
# Section 5.1: the base point B, (x, 4/5) with x positive (even).
_BX = 15112221349535400772501151409588531511454012693041857206046113283949847762202
_BY = 46316835694926478169428394003475163141307993866256225615783033603165251855960
_Point = tuple[int, int, int, int]
_B: _Point = (_BX, _BY, 1, _BX * _BY % _P)
_NEUTRAL: _Point = (0, 1, 1, 0)


def _point_add(p: _Point, q: _Point) -> _Point:
    """Section 5.1.4: the unified addition, which also doubles."""

    x1, y1, z1, t1 = p
    x2, y2, z2, t2 = q
    a = (y1 - x1) * (y2 - x2) % _P
    b = (y1 + x1) * (y2 + x2) % _P
    c = t1 * 2 * _D * t2 % _P
    d = z1 * 2 * z2 % _P
    e, f, g, h = b - a, d - c, d + c, b + a
    return (e * f % _P, g * h % _P, f * g % _P, e * h % _P)


def _point_multiply(scalar: int, point: _Point) -> _Point:
    """Section 5.1.4: [scalar]point by double and add."""

    result = _NEUTRAL
    while scalar > 0:
        if scalar & 1:
            result = _point_add(result, point)
        point = _point_add(point, point)
        scalar >>= 1
    return result


def _point_equal(p: _Point, q: _Point) -> bool:
    # x1/z1 == x2/z2 and y1/z1 == y2/z2, without dividing.
    return (p[0] * q[2] - q[0] * p[2]) % _P == 0 and (p[1] * q[2] - q[1] * p[2]) % _P == 0


def _point_decode(raw: bytes) -> _Point | None:
    """Section 5.1.3: the point a 32-byte encoding names, or None for any other spelling."""

    if len(raw) != 32:
        return None
    value = int.from_bytes(raw, "little")
    sign, y = value >> 255, value & ((1 << 255) - 1)
    # Step 1: y is the low 255 bits and must be below p, so y and y + p never
    # both name a point.
    if y >= _P:
        return None
    # Step 2: x^2 = u / v with u = y^2 - 1 and v = d y^2 + 1; the candidate root
    # is x = u v^3 (u v^7)^((p - 5) / 8).
    u = (y * y - 1) % _P
    v = (_D * y * y + 1) % _P
    x = u * pow(v, 3, _P) * pow(u * pow(v, 7, _P), (_P - 5) // 8, _P) % _P
    # Step 3: the candidate, the candidate times sqrt(-1), or no square root at all.
    check = v * x * x % _P
    if check != u:
        if check != (-u) % _P:
            return None
        x = x * _SQRT_M1 % _P
    # Step 4: x = 0 has no negative, so a set sign bit is a second spelling of it.
    if x == 0 and sign == 1:
        return None
    if x & 1 != sign:
        x = _P - x
    return (x, y, 1, x * y % _P)


def ed25519_verify(public_key: bytes, message: bytes, signature: bytes) -> bool:
    """Section 5.1.7: whether ``signature`` is the key's Ed25519 signature of ``message``.

    Never raises; any malformed input is False. Both points must decode
    canonically (section 5.1.3), S must be below L, and the group equation is
    checked without the cofactor, as section 5.1.7 permits.
    """

    if not all(isinstance(value, bytes) for value in (public_key, message, signature)):
        return False
    if len(public_key) != 32 or len(signature) != 64:
        return False
    # Step 1: decode A and R, and refuse a non-canonical S (S + kL is the same scalar).
    a, r = _point_decode(public_key), _point_decode(signature[:32])
    s = int.from_bytes(signature[32:], "little")
    if a is None or r is None or s >= _L:
        return False
    # Step 2: h = SHA-512(R || A || M) mod L. Plain Ed25519 has no dom2 prefix.
    digest = hashlib.sha512(signature[:32] + public_key + message).digest()
    h = int.from_bytes(digest, "little") % _L
    # Step 3: [S]B == R + [h]A.
    return _point_equal(_point_multiply(s, _B), _point_add(r, _point_multiply(h, a)))


# --- the approval ticket reader (ADR 0271 decision 3) --------------------------------


def _canonical_json(value: object, *, ascii_only: bool) -> str:
    """The production spelling: sorted keys, ``,`` and ``:``, no NaN. Raises ValueError."""

    return json.dumps(
        value, sort_keys=True, separators=(",", ":"), ensure_ascii=ascii_only, allow_nan=False
    )


def execution_arguments_sha256(arguments: Mapping[str, Any]) -> str:
    """The digest a ticket signs: the arguments without the top-level ``approval_ticket``.

    Canonical JSON with literal UTF-8 (no ASCII escaping), then SHA-256, as
    ``curie_internal.approver_ticket.execution_arguments_sha256``. A nested
    ``approval_ticket`` member is execution data and stays. Raises ValueError
    for arguments that are not an object or cannot be encoded.
    """

    if type(arguments) is not dict:
        raise ValueError("the arguments are not an object")
    execution = {key: value for key, value in arguments.items() if key != "approval_ticket"}
    try:
        text = _canonical_json(execution, ascii_only=False)
        return hashlib.sha256(text.encode("utf-8")).hexdigest()
    except (TypeError, RecursionError):
        raise ValueError("the arguments have no canonical encoding") from None


def _segment(text: str) -> bytes | None:
    """The bytes of one canonical unpadded base64url segment, or None."""

    if _URL_SAFE.fullmatch(text) is None:
        return None
    try:
        raw = base64.urlsafe_b64decode(text + "=" * (-len(text) % 4))
    except (binascii.Error, ValueError):
        return None
    # Nonzero trailing pad bits decode too; only the canonical spelling re-encodes.
    return raw if _b64url(raw) == text else None


def _ticket_text(value: object, low: int, high: int, *, utf8_bytes: bool = False) -> bool:
    """A ticket string: its bound and the model's text hygiene.

    It must encode as UTF-8 (no lone surrogate), have ``low`` to ``high`` code
    points (UTF-8 bytes with ``utf8_bytes``), carry no C0 or C1 control
    (U+0000 to U+001F, U+007F to U+009F) and no leading or trailing whitespace
    as ``str.strip`` reads it, exactly as ``approver_ticket._clean_text``.
    """

    if type(value) is not str:
        return False
    try:
        encoded = value.encode("utf-8")
    except UnicodeEncodeError:
        return False
    if not low <= (len(encoded) if utf8_bytes else len(value)) <= high:
        return False
    if value != value.strip():
        return False
    return not any(ord(c) < 32 or 127 <= ord(c) <= 159 for c in value)


def _ticket_uuid(value: object) -> bool:
    return type(value) is str and _UUID.fullmatch(value) is not None


def _ticket_timestamp(value: object) -> bool:
    # Booleans are not integers here.
    return type(value) is int and 0 <= value <= MAX_TIMESTAMP


def _ticket_person(value: dict[str, Any]) -> bool:
    return (
        _ticket_uuid(value["tenant_id"])
        and _ticket_text(value["issuer"], 1, ISSUER_MAX_CHARS)
        and _ticket_text(value["subject"], 1, SUBJECT_MAX_CHARS)
    )


def _ticket_requester(value: object) -> bool:
    """``CanonicalPrincipal``: exactly tenant_id, issuer and subject."""

    return type(value) is dict and set(value) == _PRINCIPAL_FIELDS and _ticket_person(value)


def _ticket_account(value: object) -> bool:
    """``RecipientAccount`` and its ``RecipientNamespace``, both closed."""

    if type(value) is not dict or set(value) != _ACCOUNT_FIELDS:
        return False
    namespace = value["namespace"]
    return (
        _ticket_text(value["provider"], 1, IDENTIFIER_MAX_CHARS)
        and _ticket_uuid(value["installation_id"])
        and _ticket_text(value["account_id"], 1, IDENTIFIER_MAX_CHARS)
        and type(namespace) is dict
        and set(namespace) == _NAMESPACE_FIELDS
        and _ticket_uuid(namespace["id"])
        and _ticket_text(namespace["authority"], 0, AUTHORITY_MAX_CHARS)
        and _ticket_text(namespace["kind"], 1, IDENTIFIER_MAX_CHARS)
        and _ticket_text(namespace["key"], 1, IDENTIFIER_MAX_CHARS)
    )


def _ticket_approver(value: object) -> bool:
    """``SelectedApprover``: a principal, plus 1 to 16 recipient accounts when present."""

    if type(value) is not dict or not _PRINCIPAL_FIELDS <= set(value) <= _APPROVER_FIELDS:
        return False
    if not _ticket_person(value):
        return False
    if "recipient_accounts" not in value:
        return True
    # An absent list is omitted, never null.
    accounts = value["recipient_accounts"]
    return (
        type(accounts) is list
        and 1 <= len(accounts) <= MAX_RECIPIENT_ACCOUNTS
        and all(_ticket_account(account) for account in accounts)
    )


def _ticket_claims(claims: object) -> bool:
    """Whether parsed claims have ``ApprovalTicket``'s closed shape, types and bounds."""

    if type(claims) is not dict or set(claims) != _TICKET_FIELDS:
        return False
    nonce, digest, approvers = claims["nonce"], claims["arguments_sha256"], claims["approvers"]
    issued_at, expires_at = claims["issued_at"], claims["expires_at"]
    shaped = (
        type(claims["version"]) is int
        and claims["version"] == 1
        and claims["purpose"] == "approval_request"
        and _ticket_uuid(claims["tenant_id"])
        and _ticket_uuid(claims["agent_id"])
        and all(
            _ticket_text(claims[field], 1, SHORT_TEXT_MAX_BYTES, utf8_bytes=True)
            for field in ("connector", "tool", "route", "key_id")
        )
        and _ticket_requester(claims["requester"])
        and type(nonce) is str
        and len(_segment(nonce) or b"") == NONCE_BYTES
        and _ticket_timestamp(issued_at)
        and _ticket_timestamp(expires_at)
        and type(digest) is str
        and _DIGEST.fullmatch(digest) is not None
        and type(approvers) is list
        and 1 <= len(approvers) <= MAX_APPROVERS
        and all(_ticket_approver(approver) for approver in approvers)
    )
    if not shaped:
        return False
    # The model's cross-field rules: a preparation lifetime of at most five
    # minutes, distinct approvers, and one tenant for every principal.
    if not 0 < expires_at - issued_at <= MAX_PREPARATION_S:
        return False
    people = [(p["tenant_id"], p["issuer"], p["subject"]) for p in approvers]
    tenant = claims["tenant_id"]
    return (
        len(set(people)) == len(people)
        and claims["requester"]["tenant_id"] == tenant
        and all(person[0] == tenant for person in people)
    )


def read_ticket(value: object, public_key: bytes) -> dict[str, Any] | None:
    """The claims of one ``cat1`` approval ticket signed by ``public_key``, or None.

    Never raises, and consults no clock. The checks run in this order: the
    framing (at most 16,384 ASCII characters, three segments, the ``cat1``
    prefix, canonical unpadded base64url, a 64-byte signature), the Ed25519
    signature over the ASCII ``cat1.<payload>`` bytes, strict ASCII JSON (no
    duplicate key, NaN or Infinity), the closed claim shape, and finally that
    re-encoding the claims (sorted keys, ``,`` and ``:``, ASCII escaping)
    gives back exactly the payload bytes. Key id, scope, requester and argument
    digest are the calling tool's checks; the ticket's own key id never selects
    the key.
    """

    if type(value) is not str or not 0 < len(value) <= MAX_TICKET_CHARS or not value.isascii():
        return None
    parts = value.split(".")
    if len(parts) != 3 or parts[0] != TICKET_PREFIX:
        return None
    body, signature = _segment(parts[1]), _segment(parts[2])
    if body is None or signature is None or len(signature) != 64:
        return None
    signed = (TICKET_PREFIX + "." + parts[1]).encode("ascii")
    if not ed25519_verify(public_key, signed, signature):
        return None
    if not body.isascii():
        return None
    try:
        claims = _strict_json(body)
    except ValueError:
        return None
    if not _ticket_claims(claims):
        return None
    try:
        if _canonical_json(claims, ascii_only=True).encode("ascii") != body:
            return None
    except (ValueError, TypeError, RecursionError):
        return None
    return dict(claims)


def _ticket_public_key(text: str) -> bytes:
    """``TICKET_PUBLIC_KEY``: one 32-byte key in canonical standard base64. Raises ValueError."""

    try:
        raw = base64.b64decode(text, validate=True)
    except (binascii.Error, ValueError):
        raise ValueError("TICKET_PUBLIC_KEY is not standard base64") from None
    if len(raw) != 32 or base64.b64encode(raw).decode("ascii") != text:
        raise ValueError("TICKET_PUBLIC_KEY is not one 32-byte key in standard base64")
    return raw


class Tickets:
    """What ``execute_approved`` holds a ticket to. Unconfigured while any setting is blank."""

    def __init__(
        self, *, public_key: str, key_id: str, route: str, agent_id: str, connector: str
    ) -> None:
        settings = (public_key, key_id, route, agent_id, connector)
        if not all(isinstance(setting, str) for setting in settings):
            raise ValueError("the ticket settings are strings")
        self.configured = all((public_key, key_id, route, agent_id))
        self._public_key = _ticket_public_key(public_key) if public_key else b""
        self._scope = (key_id, connector or CONNECTOR_NAME, APPROVED_TOOL, route, agent_id)

    def refusal(self, arguments: Mapping[str, Any], principal: Mapping[str, str]) -> str | None:
        """The ticket refusal code for this call, or None when its ticket holds."""

        ticket = arguments.get("approval_ticket")
        if not isinstance(ticket, str):
            return TICKET_MISSING
        if not self.configured:
            return TICKET_UNCONFIGURED
        claims = read_ticket(ticket, self._public_key)
        if claims is None:
            return TICKET_INVALID
        if tuple(claims[field] for field in _SCOPE_FIELDS) != self._scope:
            return TICKET_SCOPE_MISMATCH
        # Exact comparison: no case folding, trimming or normalization.
        if claims["requester"] != dict(principal):
            return TICKET_REQUESTER_MISMATCH
        try:
            digest = execution_arguments_sha256(arguments)
        except ValueError:
            return TICKET_ARGUMENTS_MISMATCH
        return None if digest == claims["arguments_sha256"] else TICKET_ARGUMENTS_MISMATCH


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


def _log(tool: str, outcome: str, requester: str | None, grant: str | None) -> None:
    try:
        sys.stderr.write(
            f"{tool} outcome={outcome} requester={requester or '-'} grant={grant or '-'}\n"
        )
        sys.stderr.flush()
    except (AttributeError, OSError, ValueError):
        pass


class HumanRestrictedServer(ThreadingHTTPServer):
    """One thread per connection; ``ledger`` holds the applied effects in order."""

    daemon_threads = True
    # A keep-alive connection a client left open must not hold up server_close.
    block_on_close = False

    def __init__(self, address: tuple[str, int], authority: Authority, tickets: Tickets) -> None:
        super().__init__(address, _Handler)
        self.authority = authority
        self.tickets = tickets
        self.ledger: list[dict[str, Any]] = []
        # The grant ids that applied an effect: each applies at most one, on either
        # protected tool. Shared with the ledger's lock.
        self._granted: set[str] = set()
        self._ledger_lock = threading.Lock()

    def apply_effect(self, requester: str) -> int:
        with self._ledger_lock:
            n = len(self.ledger) + 1
            self.ledger.append({"n": n, "requester": requester})
            return n

    def apply_granted_effect(self, requester: str, grant: str) -> int | None:
        """The new effect's number, or None when ``grant`` already applied one."""

        with self._ledger_lock:
            if grant in self._granted:
                return None
            self._granted.add(grant)
            n = len(self.ledger) + 1
            self.ledger.append({"n": n, "requester": requester, "grant": fingerprint(grant)})
            return n

    def grant_applied(self, grant: str) -> bool:
        with self._ledger_lock:
            return grant in self._granted

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

    def _tool_result(self, request_id: str | int, text: str, *, is_error: bool) -> None:
        self._result(request_id, {"content": [{"type": "text", "text": text}], "isError": is_error})

    def _drop_connection(self) -> None:
        """End the request with no response at all: the caller cannot learn the outcome."""

        self.close_connection = True
        try:
            self.connection.shutdown(socket.SHUT_RDWR)
        except OSError:
            pass

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
            self._result(request_id, {"tools": TOOLS})
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
        tool = params.get("name")
        if tool not in TOOL_NAMES:
            self._error(200, request_id, INVALID_PARAMS, "unknown tool")
            return
        arguments = params.get("arguments")
        if arguments is None:
            arguments = {}
        if not isinstance(arguments, dict) or not isinstance(arguments.get("note", ""), str):
            self._error(200, request_id, INVALID_PARAMS, "arguments take an optional string note")
            return
        # The arguments are checked for shape and otherwise unused for identity:
        # nothing a model writes names, grants or redirects the requester.
        if tool == TOOL_NAME:
            outcome, requester, n = self._record_effect()
            _log(TOOL_NAME, outcome, requester, None)
            if outcome == OK:
                self._tool_result(request_id, f"effect={n} requester={requester}", is_error=False)
            else:
                self._tool_result(request_id, f"refused {outcome}", is_error=True)
            return
        drop_reply = arguments.get("drop_reply", False)
        if not isinstance(drop_reply, bool):
            self._error(200, request_id, INVALID_PARAMS, "drop_reply is a boolean")
            return
        outcome, requester, grant, n = self._execute(str(tool), arguments)
        _log(str(tool), outcome, requester, grant)
        if outcome != OK:
            self._tool_result(request_id, f"refused {outcome}", is_error=True)
        elif drop_reply:
            self._drop_connection()
        else:
            text = f"effect={n} requester={requester} grant={grant}"
            self._tool_result(request_id, text, is_error=False)

    def _requester(self) -> tuple[str, dict[str, str]] | str:
        """The proxy's ``X-Curie-Principal`` value and its principal, or a refusal code."""

        if not from_caller_proxy(str(self.client_address[0])):
            return MISSING_REQUESTER
        values = self.headers.get_all(PRINCIPAL_HEADER) or []
        if not values:
            return MISSING_REQUESTER
        if len(values) != 1:
            return REQUESTER_INVALID
        principal = read_principal_header(values[0])
        if principal is None:
            return REQUESTER_INVALID
        return values[0], principal

    def _record_effect(self) -> tuple[str, str | None, int | None]:
        """``(outcome, requester fingerprint, effect number)`` for this request alone."""

        found = self._requester()
        if isinstance(found, str):
            return found, None, None
        header, principal = found
        requester = fingerprint(header)
        decision = self.server.authority.decide(principal)
        if decision is None:
            return AUTHORITY_UNAVAILABLE, requester, None
        if decision != "allow":
            return NOT_AUTHORIZED, requester, None
        return OK, requester, self.server.apply_effect(requester)

    def _execute(
        self, tool: str, arguments: Mapping[str, Any]
    ) -> tuple[str, str | None, str | None, int | None]:
        """``(outcome, requester and grant fingerprints, effect number)`` for this request alone.

        Attestation, requester, the ticket (``execute_approved`` only), current
        authority, then the effect.
        """

        values = self.headers.get_all(GRANT_VERIFIED_HEADER) or []
        if not from_caller_proxy(str(self.client_address[0])) or not values:
            return ATTESTATION_MISSING, None, None, None
        grant = values[0]
        if len(values) != 1 or not isinstance(grant, str) or _UUID.fullmatch(grant) is None:
            return ATTESTATION_INVALID, None, None, None
        if self.server.grant_applied(grant):
            return GRANT_REPLAYED, None, None, None
        held = fingerprint(grant)
        found = self._requester()
        if isinstance(found, str):
            return found, None, held, None
        header, principal = found
        requester = fingerprint(header)
        if tool == APPROVED_TOOL:
            refusal = self.server.tickets.refusal(arguments, principal)
            if refusal is not None:
                return refusal, requester, held, None
        decision = self.server.authority.decide(principal)
        if decision is None:
            return AUTHORITY_UNAVAILABLE, requester, held, None
        if decision != "allow":
            return NOT_AUTHORIZED, requester, held, None
        n = self.server.apply_granted_effect(requester, grant)
        if n is None:
            # Another call carrying this grant id applied while this one asked authority.
            return GRANT_REPLAYED, requester, held, None
        return OK, requester, held, n


def make_server(
    host: str,
    port: int,
    *,
    authority_url: str,
    authority_bearer: str,
    timeout_s: float = 2.0,
    ticket_public_key: str = "",
    ticket_key_id: str = "",
    ticket_route: str = "",
    ticket_agent_id: str = "",
    ticket_connector: str = CONNECTOR_NAME,
) -> ThreadingHTTPServer:
    """The connector bound to ``(host, port)``, not yet serving. ValueError on bad config.

    Blank ticket settings are not bad config: ``execute_approved`` then refuses
    ``ticket_unconfigured``. A public key that is set but not one 32-byte key
    in standard base64 is.
    """

    authority = Authority(authority_url, authority_bearer, timeout_s)
    tickets = Tickets(
        public_key=ticket_public_key,
        key_id=ticket_key_id,
        route=ticket_route,
        agent_id=ticket_agent_id,
        connector=ticket_connector,
    )
    return HumanRestrictedServer((host, port), authority, tickets)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        prog="human_restricted_connector",
        description=(
            "The reference human-restricted hosted connector. Configured by AUTHORITY_URL, "
            f"AUTHORITY_BEARER and PORT (default {DEFAULT_PORT}), and for execute_approved by "
            "TICKET_PUBLIC_KEY, TICKET_KEY_ID, TICKET_ROUTE, TICKET_AGENT_ID and "
            f"TICKET_CONNECTOR (default {CONNECTOR_NAME})."
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
            ticket_public_key=os.environ.get("TICKET_PUBLIC_KEY", ""),
            ticket_key_id=os.environ.get("TICKET_KEY_ID", ""),
            ticket_route=os.environ.get("TICKET_ROUTE", ""),
            ticket_agent_id=os.environ.get("TICKET_AGENT_ID", ""),
            ticket_connector=os.environ.get("TICKET_CONNECTOR") or CONNECTOR_NAME,
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
