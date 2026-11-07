"""Server-controlled delivery metadata, separate from the reply event wire."""

from collections.abc import Hashable
from typing import Protocol


class ReplyRoute(Hashable, Protocol):
    @property
    def endpoint(self) -> str | None: ...

    @property
    def adapter(self) -> str | None: ...
