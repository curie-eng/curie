"""Navigation policy shared by the renderer and worker behavior packs."""

from collections.abc import Sequence
from dataclasses import dataclass
from typing import Protocol


class Navigation(Protocol):
    @property
    def enabled(self) -> bool: ...
    @property
    def hub_label(self) -> str: ...
    @property
    def hub_command(self) -> str: ...


@dataclass(frozen=True)
class ReplyNavigation:
    enabled: bool
    hub_label: str
    hub_command: str


def navigation_buttons(
    nav: Navigation, buttons: Sequence[tuple[str, str]]
) -> list[tuple[str, str]]:
    result = list(buttons)
    if not nav.enabled or not nav.hub_command:
        return result
    if any(command == nav.hub_command for _, command in result):
        return result
    arrow = "↑" if any(label.lstrip().startswith("←") for label, _ in result) else "←"
    result.append((f"{arrow} {nav.hub_label}".strip(), nav.hub_command))
    return result
