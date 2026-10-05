"""Keep channel message bodies out of the persisted turn record (ADR 0100 section 7).

A successful ``curie-slack`` read result is replaced, in the portable record
only, by a provenance stub: message ids, thread ids, timestamps and permalinks,
never text or authors. Error results carry no body and are kept verbatim. The
model's in-session context is unchanged; it is request lifetime cache.
"""

from __future__ import annotations

import json
from collections.abc import Mapping
from typing import Any, Final

from ..history import ConversationMessage

STUB_NOTE: Final = "Channel message bodies are not retained after the turn."
_KEPT_FIELDS: Final = ("id", "thread_id", "timestamp", "provenance")


def _result_text(content: Any) -> str:
    if isinstance(content, str):
        return content
    if isinstance(content, list):
        return "".join(
            str(item.get("text") or "")
            for item in content
            if isinstance(item, Mapping) and item.get("type") == "text"
        )
    return ""


def _stub(content: Any) -> str:
    try:
        page: Any = json.loads(_result_text(content))
    except ValueError:
        page = None
    if not isinstance(page, dict):
        page = {}
    raw = page.get("messages")
    messages = [
        {field: item[field] for field in _KEPT_FIELDS if isinstance(item.get(field), str)}
        for item in (raw if isinstance(raw, list) else [])
        if isinstance(item, Mapping)
    ]
    stub: dict[str, Any] = {
        "bodies_retained": False,
        "note": STUB_NOTE,
        "messages": messages,
        "has_more": page.get("has_more") is True,
    }
    if isinstance(page.get("next_cursor"), str):
        stub["next_cursor"] = page["next_cursor"]
    return json.dumps(stub)


def strip_channel_bodies(
    message: ConversationMessage, call_ids: set[str]
) -> ConversationMessage:
    """``message`` with every successful channel read result stubbed."""

    if not call_ids or not isinstance(message.content, list):
        return message
    changed = False
    blocks: list[dict[str, Any]] = []
    for block in message.content:
        if (
            block.get("type") == "tool_result"
            and block.get("tool_use_id") in call_ids
            and block.get("is_error") is not True
        ):
            block = {**block, "content": [{"type": "text", "text": _stub(block.get("content"))}]}
            changed = True
        blocks.append(dict(block))
    if not changed:
        return message
    return ConversationMessage(
        role=message.role, content=blocks, assistant_group=message.assistant_group
    )
