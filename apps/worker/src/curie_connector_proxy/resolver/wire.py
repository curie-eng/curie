"""The resolver's wire: one request shape, fixed refusal codes (ADR 0239).

"Bundle, identity and request contract" 4 makes HTTP over a pod-local Unix
socket the language-neutral resolver protocol: ``POST /v1/resolve`` carries an
invocation handle, a declared slot and an audit-only purpose. Every answer is
JSON the resolver builds itself. A refusal is ``{"refusal": <code>}`` and never
echoes a request value, so neither a handle nor a purpose can be reflected back
to a caller or into an intermediary's log.

Request parsing is strict on purpose. Exactly three string keys, no
duplicates, UTF-8 only, and the purpose bounded and free of control
characters: a connector built in another language implements this from the
corpus, and every input it can send has exactly one meaning here.
"""

from __future__ import annotations

import json
import unicodedata
from typing import Any

RESOLVE_PATH = "/v1/resolve"
MAX_REQUEST_BYTES = 65536
PURPOSE_MAX_BYTES = 256
SOCKET_ENV = "CURIE_SHARED_RESOLVER_SOCKET"

# Every code the resolver can answer, and the status it answers it with.
REFUSAL_STATUS = {
    "malformed": 400,
    "request_too_large": 413,
    "unsupported_media_type": 415,
    "not_found": 404,
    "method_not_allowed": 405,
    "length_required": 411,
    "headers_too_large": 431,
    "handle_missing": 401,
    "handle_unknown": 401,
    "handle_expired": 401,
    "slot_unknown": 403,
    "foreign_principal": 403,
    "installation_mismatch": 403,
    "application_unknown": 403,
    "not_member": 403,
    "policy_generation_mismatch": 403,
    "application_disabled": 403,
    "binding_disabled": 403,
    "binding_missing": 403,
    "custody_mismatch": 403,
    "slot_unbound": 403,
    "source_stale": 409,
    "recovery_required": 409,
    "refresh_busy": 409,
    "refresh_stale": 409,
    "internal_error": 500,
    "store_unavailable": 503,
    "key_unavailable": 503,
    "source_unavailable": 503,
    "refresh_failed": 503,
}

_REQUEST_KEYS = frozenset({"handle", "slot", "purpose"})


class ResolveRefusal(Exception):
    """A refusal on the wire. ``str(exc)`` is the code and nothing else."""

    def __init__(self, code: str) -> None:
        super().__init__(code)
        self.code = code

    def __str__(self) -> str:
        return self.code


def status_for(code: str) -> int:
    """The HTTP status of ``code``; an unknown code is an internal error."""

    return REFUSAL_STATUS.get(code, REFUSAL_STATUS["internal_error"])


def refusal_body(code: str) -> bytes:
    """The fixed refusal body. A code outside the table answers ``internal_error``."""

    known = code if code in REFUSAL_STATUS else "internal_error"
    return json.dumps({"refusal": known}, separators=(",", ":")).encode("ascii")


def _no_duplicates(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    keys = [key for key, _ in pairs]
    if len(set(keys)) != len(keys):
        raise ValueError("duplicate key")
    return dict(pairs)


def _no_constant(_: str) -> Any:
    raise ValueError("non-JSON constant")


def _purpose_ok(purpose: str) -> bool:
    if not purpose.strip():
        return False
    if len(purpose.encode("utf-8")) > PURPOSE_MAX_BYTES:
        return False
    return not any(unicodedata.category(char) == "Cc" for char in purpose)


def parse_request(body: bytes) -> tuple[str, str, str]:
    """The request's ``(handle, slot, purpose)``, or a ``ResolveRefusal``.

    An absent or empty ``handle`` is ``handle_missing``; every other shape
    error, including an extra key, a missing slot or purpose and a non-string
    value, is ``malformed``. The handle's own shape is the registry's to judge,
    so a well-formed request with a bad handle is ``handle_unknown`` there.
    """

    try:
        # Strict UTF-8 first: json.loads would otherwise sniff UTF-16 and -32.
        text = body.decode("utf-8")
        document = json.loads(text, object_pairs_hook=_no_duplicates, parse_constant=_no_constant)
    except ValueError:
        raise ResolveRefusal("malformed") from None
    if not isinstance(document, dict) or not set(document) <= _REQUEST_KEYS:
        raise ResolveRefusal("malformed")
    slot = document.get("slot")
    purpose = document.get("purpose")
    if not isinstance(slot, str) or not isinstance(purpose, str) or not _purpose_ok(purpose):
        raise ResolveRefusal("malformed")
    if "handle" not in document:
        raise ResolveRefusal("handle_missing")
    handle = document["handle"]
    if not isinstance(handle, str):
        raise ResolveRefusal("malformed")
    if not handle:
        raise ResolveRefusal("handle_missing")
    return handle, slot, purpose
