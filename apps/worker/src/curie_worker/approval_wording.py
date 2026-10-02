"""Presentation-only action wording; ledger and authorization names stay exact."""

from __future__ import annotations

import json
import re
from typing import Any

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
# A period followed by whitespace, closing punctuation or end is sentence prose.
_REFERENCE_END = r"(?![\w/-]|\.(?=[^\s\)\]\}\"'`]))"
_MCP_REFERENCE = re.compile(r"(?<![\w./-])mcp__[\w-]+" + _REFERENCE_END, re.UNICODE)


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
        reference = re.compile(r"(?<![\w./-])" + re.escape(tool) + _REFERENCE_END)
        text = reference.sub(lambda _: action_label(tool), text)
    return text


_MACHINE_PREFIX = "Tool call awaiting approval: "
_DISPLAY_LIMIT = 2400


def _value(value: Any) -> str:
    if isinstance(value, (dict, list)):
        # Nested keys and JSON types are requested content, not action metadata.
        return json.dumps(value, ensure_ascii=False)
    if value is None:
        return "none"
    if isinstance(value, bool):
        return "yes" if value else "no"
    return str(value) or "(empty)"


def describe_approval(tool: object, arguments: dict[str, Any] | None) -> str:
    """Render values without deriving approval authority from display wording."""
    text = f"Approve {action_label(tool)}."
    if arguments:
        details = "; ".join(
            f"{action_label(key).capitalize()}: {_value(value)}"
            for key, value in sorted(arguments.items())
        )
        text += " " + details
    elif arguments is None:
        text += " Details are incomplete; review the original request before approving."
    if len(text) > _DISPLAY_LIMIT:
        text = (
            text[:_DISPLAY_LIMIT]
            + "… Details exceed this preview; review the original request before approving."
        )
    return text


def approval_display(
    summary: str, tool: object = None, arguments: dict[str, Any] | None = None
) -> str:
    """Handle historical permission summaries for display only, never grants."""
    if summary.startswith(_MACHINE_PREFIX):
        name, _, payload = summary[len(_MACHINE_PREFIX) :].partition(" ")
        if arguments is None:
            try:
                decoded = json.loads(payload)
            except (TypeError, ValueError):
                decoded = None
            arguments = decoded if isinstance(decoded, dict) else None
        return describe_approval(tool or name, arguments)
    return presentation_text(summary, tool)
