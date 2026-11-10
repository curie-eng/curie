"""Caller-facing action wording; authorization names stay exact."""

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
