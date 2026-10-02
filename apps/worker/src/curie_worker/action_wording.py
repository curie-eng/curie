"""Presentation-only action wording; ledger and authorization names stay exact."""

from __future__ import annotations

import re

_NATIVE = {
    "Bash": "shell request",
    "Skill": "instruction request",
    "Read": "read file",
    "Write": "write file",
    "Edit": "edit file",
    "MultiEdit": "edit files",
    "Glob": "find files",
    "Grep": "search files",
    "WebFetch": "fetch web page",
    "WebSearch": "search web",
}
_IDENTIFIER = re.compile(r"[\w-]+", re.UNICODE)
# A filename or path containing an identifier is content, not a tool reference.
_MCP_REFERENCE = re.compile(r"(?<![\w./-])mcp__[\w-]+(?![\w./-])", re.UNICODE)


def action_label(tool: object) -> str:
    """Name the action without asserting it ran or succeeded."""

    if not isinstance(tool, str) or not _IDENTIFIER.fullmatch(tool):
        return "action"
    if tool.startswith("mcp__"):
        parts = tool.split("__")
        if len(parts) < 3 or any(not part for part in parts):
            return "action"
        tool = parts[-1]
    elif tool in _NATIVE:
        return _NATIVE[tool]
    tool = re.sub(r"([A-Z]+)([A-Z][a-z])", r"\1 \2", tool)
    tool = re.sub(r"([a-z0-9])([A-Z])", r"\1 \2", tool)
    return " ".join(tool.replace("_", " ").replace("-", " ").lower().split()) or "action"


def presentation_text(text: str, tool: object) -> str:
    """Normalize identifier references only in connector display metadata."""

    text = _MCP_REFERENCE.sub(lambda match: action_label(match.group()), text)
    if isinstance(tool, str) and tool in _NATIVE:
        # A native word ("Read permissions") is ordinary prose unless code quoted.
        return text.replace(f"`{tool}`", f"`{action_label(tool)}`")
    if isinstance(tool, str) and tool and not tool.startswith("mcp__"):
        reference = re.compile(r"(?<![\w./-])" + re.escape(tool) + r"(?![\w./-])")
        text = reference.sub(lambda _: action_label(tool), text)
    return text
