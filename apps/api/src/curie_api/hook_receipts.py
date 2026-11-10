"""Ordinary hook receipt evidence. @spec HOOK-RECEIPT-2, HOOK-RECEIPT-3, HOOK-RECEIPT-4."""

from __future__ import annotations

import json
import re
from dataclasses import dataclass
from typing import Any

import redis.asyncio as redis
from aci_protocol import ToolAccess

from .delivery import DeliveryReceiptConflict

MAX_RECEIPT_BYTES = 16384
_MAX_STREAM_ID_BYTES = 41  # Two decimal uint64 components and one dash.
_STREAM_ID = re.compile(r"(?:0|[1-9][0-9]{0,19})-(?:0|[1-9][0-9]{0,19})\Z")

# TYPE/STRLEN run before GET so a corrupt value cannot force an unbounded read.
# Both keys are observed in one script, including a claim changed after the
# caller's earlier GET. No writes or repair occur on this path.
_READ_SCRIPT = """
if redis.call('TYPE', KEYS[1]).ok ~= 'string' then return {-1} end
if redis.call('STRLEN', KEYS[1]) > 128 then return {-1} end
if redis.call('GET', KEYS[1]) ~= ARGV[1] then return {-1} end
local kind = redis.call('TYPE', KEYS[2]).ok
if kind == 'none' then return {0} end
if kind ~= 'string' then return {-1} end
if redis.call('STRLEN', KEYS[2]) > tonumber(ARGV[2]) then return {-1} end
return {1, redis.call('GET', KEYS[2])}
"""


@dataclass(frozen=True)
class HookReceipt:
    """@spec HOOK-RECEIPT-2."""

    stream_id: str
    conversation_id: str
    tool_access: ToolAccess | None


def _unique_pairs(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    """@spec HOOK-RECEIPT-2."""
    value: dict[str, Any] = {}
    for key, item in pairs:
        if key in value:
            raise ValueError("duplicate receipt member")
        value[key] = item
    return value


def _nonfinite(_value: str) -> Any:
    """@spec HOOK-RECEIPT-2."""
    raise ValueError("nonfinite receipt value")


def _stream_id(value: object) -> bool:
    """@spec HOOK-RECEIPT-2."""
    return (
        type(value) is str
        and _STREAM_ID.fullmatch(value) is not None
        and all(int(part) <= 2**64 - 1 for part in value.split("-"))
    )


def receipt_prefix(conversation_id: str, tool_access: ToolAccess | None) -> str:
    """@spec HOOK-RECEIPT-2: validate before any claim or quota write."""
    if not conversation_id or tool_access not in (None, ToolAccess.READ_ONLY):
        raise ValueError("invalid hook receipt")
    conversation_id.encode("utf-8")
    prefix = json.dumps(
        {"version": 1, "conversation_id": conversation_id, "tool_access": tool_access},
        ensure_ascii=True,
        separators=(",", ":"),
    )[:-1] + ',"stream_id":"'
    if len(prefix.encode("utf-8")) + _MAX_STREAM_ID_BYTES + 2 > MAX_RECEIPT_BYTES:
        raise ValueError("hook receipt exceeds its storage bound")
    return prefix


def _decode(raw: bytes | str, current: str) -> HookReceipt:
    """@spec HOOK-RECEIPT-2, HOOK-RECEIPT-3."""
    encoded = raw.encode("utf-8") if isinstance(raw, str) else raw
    if len(encoded) > MAX_RECEIPT_BYTES:
        raise ValueError("oversized hook receipt")
    value = json.loads(
        encoded.decode("utf-8"), object_pairs_hook=_unique_pairs, parse_constant=_nonfinite
    )
    if type(value) is not dict or set(value) != {
        "version", "stream_id", "conversation_id", "tool_access"
    }:
        raise ValueError("invalid receipt members")
    if type(value["version"]) is not int or value["version"] != 1:
        raise ValueError("invalid receipt version")
    if not _stream_id(value["stream_id"]) or value["stream_id"] != current:
        raise ValueError("receipt does not match claim")
    conversation = value["conversation_id"]
    if type(conversation) is not str or not conversation:
        raise ValueError("invalid receipt conversation")
    conversation.encode("utf-8")
    policy = value["tool_access"]
    if policy is not None and (type(policy) is not str or policy != "read-only"):
        raise ValueError("invalid receipt policy")
    return HookReceipt(current, conversation, None if policy is None else ToolAccess.READ_ONLY)


async def read_receipt(client: redis.Redis, key: str, current: str) -> HookReceipt | None:
    """@spec HOOK-RECEIPT-3, HOOK-RECEIPT-4: None means absent legacy metadata only."""
    if not _stream_id(current):
        raise DeliveryReceiptConflict("invalid hook receipt claim")
    result: Any = await client.eval(
        _READ_SCRIPT, 2, key, key + ":receipt", current, MAX_RECEIPT_BYTES
    )
    if result[0] == 0:
        return None
    if result[0] != 1:
        raise DeliveryReceiptConflict("invalid hook receipt evidence")
    try:
        return _decode(result[1], current)
    except (ValueError, UnicodeError, RecursionError) as exc:
        raise DeliveryReceiptConflict("invalid hook receipt evidence") from exc
