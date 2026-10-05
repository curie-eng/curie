"""Tool identities shared by SDK registration and provider neutral approval."""

from collections.abc import Mapping
from types import MappingProxyType
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
# ``curie-slack`` server only for a ``channelRead``-granted, real-model boot. They are governed
# by toolPolicy like a connector's tools and are never platform exempt.
CHANNEL_READ_TOOL_SHORT_NAMES: Final = (
    "read_channel_history",
    "read_thread_replies",
    "read_channel_message",
)
CHANNEL_READ_TOOL_NAMES: Final[frozenset[str]] = frozenset(
    f"mcp__{CHANNEL_READ_SERVER_NAME}__{name}" for name in CHANNEL_READ_TOOL_SHORT_NAMES
)
# The canvas tools (ADR 0200, #3819) ride the same server, each behind its own
# grant. Any one grant mounts the server; history stays behind ``channelRead``.
CANVAS_LIST_TOOL_SHORT_NAME: Final = "list_channel_canvases"
CANVAS_READ_TOOL_SHORT_NAME: Final = "read_canvas"
CANVAS_EDIT_TOOL_SHORT_NAME: Final = "edit_canvas_cell"
PLATFORM_SLACK_TOOLS_BY_GRANT: Final[Mapping[str, tuple[str, ...]]] = MappingProxyType(
    {
        "channelRead": CHANNEL_READ_TOOL_SHORT_NAMES,
        "canvasList": (CANVAS_LIST_TOOL_SHORT_NAME,),
        "canvasRead": (CANVAS_READ_TOOL_SHORT_NAME,),
        "canvasEdit": (CANVAS_EDIT_TOOL_SHORT_NAME,),
    }
)


def _full_names(short_names: tuple[str, ...]) -> frozenset[str]:
    return frozenset(f"mcp__{CHANNEL_READ_SERVER_NAME}__{name}" for name in short_names)


def platform_slack_tool_names(grants: frozenset[str]) -> frozenset[str]:
    """The full names of the ``curie-slack`` tools ``grants`` mounts."""

    return frozenset().union(
        *(
            _full_names(PLATFORM_SLACK_TOOLS_BY_GRANT[grant])
            for grant in grants
            if grant in PLATFORM_SLACK_TOOLS_BY_GRANT
        )
    )


CANVAS_TOOL_NAMES: Final[frozenset[str]] = _full_names(
    (CANVAS_LIST_TOOL_SHORT_NAME, CANVAS_READ_TOOL_SHORT_NAME, CANVAS_EDIT_TOOL_SHORT_NAME)
)
# List and read only read; the cell edit writes to the provider.
CANVAS_READ_ONLY_TOOL_NAMES: Final[frozenset[str]] = _full_names(
    (CANVAS_LIST_TOOL_SHORT_NAME, CANVAS_READ_TOOL_SHORT_NAME)
)
PLATFORM_SLACK_TOOL_NAMES: Final[frozenset[str]] = CHANNEL_READ_TOOL_NAMES | CANVAS_TOOL_NAMES
