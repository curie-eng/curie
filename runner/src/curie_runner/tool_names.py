"""Tool identities shared by SDK registration and provider neutral approval."""

from typing import Final

from plugin_format import CHANNEL_READ_SERVER_NAME

STATE_SERVER_NAME: Final = "curie-state"
STATE_TOOL_SHORT_NAMES: Final = ("get", "set", "append", "list", "delete")
STATE_TOOL_NAMES: Final[frozenset[str]] = frozenset(
    f"mcp__{STATE_SERVER_NAME}__{name}" for name in STATE_TOOL_SHORT_NAMES
)
PROGRESS_TOOL: Final = "report_progress"
TURN_PROGRESS_TOOL: Final = "progress"
# The platform channel read tools (ADR 0100, #2877), mounted on the reserved
# ``curie-slack`` server only for a granted, real-model boot. They are governed
# by toolPolicy like a connector's tools and are never platform exempt.
CHANNEL_READ_TOOL_SHORT_NAMES: Final = (
    "read_channel_history",
    "read_thread_replies",
    "read_channel_message",
)
CHANNEL_READ_TOOL_NAMES: Final[frozenset[str]] = frozenset(
    f"mcp__{CHANNEL_READ_SERVER_NAME}__{name}" for name in CHANNEL_READ_TOOL_SHORT_NAMES
)
