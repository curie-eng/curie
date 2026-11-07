"""Server-controlled delivery metadata, separate from the reply event wire."""

from collections.abc import Hashable
from dataclasses import dataclass
from typing import Protocol


class ReplyRoute(Hashable, Protocol):
    @property
    def endpoint(self) -> str | None: ...

    @property
    def adapter(self) -> str | None: ...


@dataclass(frozen=True)
class ReplyDestination:
    """Gateway-owned delivery metadata; never part of a reply event."""

    endpoint: str | None = None
    adapter: str | None = None
