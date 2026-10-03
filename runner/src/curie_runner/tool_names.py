"""Tool identities shared by SDK registration and provider neutral approval."""

from typing import Final

STATE_SERVER_NAME: Final = "curie-state"
STATE_TOOL_SHORT_NAMES: Final = ("get", "set", "append", "list", "delete")
STATE_TOOL_NAMES: Final[frozenset[str]] = frozenset(
    f"mcp__{STATE_SERVER_NAME}__{name}" for name in STATE_TOOL_SHORT_NAMES
)
PROGRESS_TOOL: Final = "report_progress"
TURN_PROGRESS_TOOL: Final = "progress"
