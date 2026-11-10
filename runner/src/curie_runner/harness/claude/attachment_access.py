"""Claude tools for scoped platform attachment access; credentials never model inputs."""

from __future__ import annotations

import json
from typing import Any

from claude_agent_sdk import SdkMcpTool, tool

from ...attachment_access import AttachmentAccess, AttachmentAccessError


def build_attachment_tools(access: AttachmentAccess) -> tuple[SdkMcpTool[Any], ...]:
    async def invoke(operation: str, args: dict[str, Any]) -> dict[str, Any]:
        try:
            result = await (access.list(args) if operation == "list" else access.fetch(args))
            return {"content": [{"type": "text", "text": json.dumps(result)}]}
        except AttachmentAccessError as exc:
            return {"content": [{"type": "text", "text": str(exc)}], "is_error": True}

    @tool(
        "list_attachments",
        "List remote attachment references from this thread. No file bodies are downloaded. "
        "Use the returned IDs with fetch_attachment; cached indicates a verified local copy.",
        {
            "type": "object",
            "properties": {
                "cursor": {"type": "string"},
                "limit": {"type": "integer", "minimum": 1, "maximum": 100},
            },
            "additionalProperties": False,
        },
    )
    async def list_attachments(args: dict[str, Any]) -> dict[str, Any]:
        return await invoke("list", args)

    @tool(
        "fetch_attachment",
        "Download one listed attachment completely, or reuse its authorized verified cache. "
        "Returns an absolute local path and actual size only after integrity verification. "
        "Accepts a catalog ID, never a URL or destination path.",
        {
            "type": "object",
            "properties": {"id": {"type": "string"}},
            "required": ["id"],
            "additionalProperties": False,
        },
    )
    async def fetch_attachment(args: dict[str, Any]) -> dict[str, Any]:
        return await invoke("fetch", args)

    return (list_attachments, fetch_attachment)
