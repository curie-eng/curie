"""@spec PROTECTED-HOOK-SOURCE-4 PROTECTED-HOOK-SOURCE-9."""

from __future__ import annotations

import hashlib
import hmac
import json
import time
from uuid import UUID

from curie_internal.sandbox_token import signature

from . import hook_signing


def derive(api_key: str, *, agent_id: str, hook: str, generation: int) -> str:
    """@spec PROTECTED-HOOK-SOURCE-4."""
    material = json.dumps(
        ["curie.hook.source.v1", str(UUID(agent_id)), hook, generation],
        ensure_ascii=True,
        separators=(",", ":"),
    )
    return signature(api_key, material)


def sign_support(
    secret: str,
    *,
    timestamp: str,
    delivery_id: str,
    hook: str,
    tool_access: str | None,
    body: bytes,
) -> str:
    """@spec PROTECTED-HOOK-SOURCE-9."""
    if "." in delivery_id:
        raise ValueError("a hook delivery id may not contain '.'")
    material = b"curie.hook.support.v1\n" + hook_signing.material(
        timestamp, delivery_id, body, hook=hook, tool_access=tool_access
    )
    digest = hmac.new(secret.encode(), material, hashlib.sha256)
    return "sha256=" + digest.hexdigest()


def verify_support(
    secret: str,
    *,
    timestamp: str | None,
    delivery_id: str,
    hook: str,
    tool_access: str | None,
    body: bytes,
    header: str | None,
    now: float | None = None,
) -> bool:
    """@spec PROTECTED-HOOK-SOURCE-9."""
    if not header or not header.isascii() or not header.startswith("sha256="):
        return False
    if not timestamp or not (timestamp.isascii() and timestamp.isdigit()):
        return False
    if len(timestamp) > hook_signing.MAX_TIMESTAMP_DIGITS:
        return False
    if "." in delivery_id:
        return False
    current = time.time() if now is None else now
    if abs(current - int(timestamp)) > hook_signing.TOLERANCE_S:
        return False
    expected = sign_support(
        secret,
        timestamp=timestamp,
        delivery_id=delivery_id,
        hook=hook,
        tool_access=tool_access,
        body=body,
    )
    return hmac.compare_digest(expected, header)
