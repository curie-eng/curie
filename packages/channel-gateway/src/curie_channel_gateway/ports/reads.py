"""Provider I/O signatures; authorization and read budgets stay in the API."""

from collections.abc import Iterator
from typing import Literal, Protocol

from channel_protocol import ChannelCapability


class IdentitySelection[Route](Protocol):
    """Select a configured credential from authorized binding routes only."""

    def identity(self, routes: list[Route]) -> str: ...

    def has_identity(self, identity: str) -> bool: ...

    def identity_key(self, identity: str) -> str:
        """Return a stable non-secret name, never a token."""
        ...


class FileSource(Protocol):
    """Fetch bytes using the configured provider identity, without exposing it."""

    def fetch(self, file_id: str) -> Iterator[bytes]: ...


class SenderIdentities(Protocol):
    async def identity_of(self, author: str) -> str | None:
        """None means unresolved; preserve the current nonblocking lookup."""
        ...


class AddressIdentityLookup(Protocol):
    async def identity_for_address(self, kind: str, address: str) -> str | None: ...


class ChannelReader[Route, Window, Page, Message](IdentitySelection[Route], Protocol):
    """Bind parameters to existing API read records, without an API import.

    Window is ResolvedWindow ([oldest, latest)), Page is ProviderPage and
    Message is ChannelMessage. Preserve provider cursors and epoch-microsecond
    boundaries. The caller authorizes channel before every provider call.
    """

    capabilities: frozenset[ChannelCapability]

    def rate_limit_key(
        self, op: Literal["history", "thread", "message"], message_id: str | None
    ) -> str: ...

    def valid_thread_id(self, value: str) -> bool: ...

    def valid_message_id(self, value: str) -> bool: ...

    async def history(
        self,
        *,
        identity: str,
        channel: str,
        window: Window,
        limit: int,
        cursor: str | None,
        boundary: int | None = None,
    ) -> Page: ...

    async def thread(
        self,
        *,
        identity: str,
        channel: str,
        thread_id: str,
        window: Window,
        limit: int,
        cursor: str | None,
        boundary: int | None = None,
    ) -> Page: ...

    async def message(self, *, identity: str, channel: str, message_id: str) -> Message | None: ...


class CanvasReader[Route, File, Page, Document](IdentitySelection[Route], Protocol):
    """Bind File/Page/Document to the existing API canvas records.

    The API retains binding authorization and all policy around edits.
    """

    capabilities: frozenset[ChannelCapability]

    def valid_canvas_id(self, value: str) -> bool: ...

    def valid_section_id(self, value: str) -> bool: ...

    async def locate(self, *, identity: str, canvas_id: str) -> File | None: ...

    async def is_member(self, *, identity: str, channel: str) -> bool: ...

    async def list(self, *, identity: str, channel: str) -> Page: ...

    async def document(self, *, identity: str, file: File) -> Document: ...

    async def replace_cell(
        self, *, identity: str, canvas_id: str, section_id: str, text: str
    ) -> None: ...
