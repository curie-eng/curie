"""The channel port's read side: what a surface implements to be readable (ADR 0100).

Authorization, bounds, budgets and cursors are surface neutral and live in
``service``. A surface becomes readable by adding a reader module beside
``slack_reads`` and registering it in ``service.channel_readers``. A kind with no
reader, or whose reader does not advertise ``history-read``, is a capability
miss, never an empty page.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from typing import Protocol

from channel_protocol import ChannelCapability

from .window import Operation, ResolvedWindow


@dataclass(frozen=True)
class ChannelMessage:
    id: str
    thread_id: str | None
    timestamp: str
    author: str
    text: str
    truncated: bool
    provenance: str
    reply_count: int | None


@dataclass(frozen=True)
class ProviderPage:
    """One page, and how to continue it: the provider's own cursor, or, when
    the provider says there is more but offers none, the ``boundary`` timestamp
    (epoch microseconds) the next page stops before (history) or starts after
    (thread).
    """

    messages: list[ChannelMessage]
    next_cursor: str | None = None
    boundary: int | None = None

    @property
    def has_more(self) -> bool:
        return self.next_cursor is not None or self.boundary is not None


@dataclass(frozen=True)
class BindingRoute:
    """The parts of an ``agent_channels`` row a reader picks its credential from."""

    adapter: str | None
    endpoint: str | None


class ChannelReader(Protocol):
    """Read operations of one surface. The caller has already authorized the
    channel; a reader never selects one from a message or thread id."""

    capabilities: frozenset[ChannelCapability]

    def identity(self, routes: list[BindingRoute]) -> str:
        """The credential identity a binding's reads are made as."""

    def has_identity(self, identity: str) -> bool: ...

    def identity_key(self, identity: str) -> str:
        """A stable, non secret name for the credential behind ``identity``."""

    def rate_limit_key(self, op: Operation, message_id: str | None) -> str:
        """The provider method an operation calls, which a cooldown is keyed by."""

    def valid_thread_id(self, value: str) -> bool: ...

    def valid_message_id(self, value: str) -> bool: ...

    async def history(
        self,
        *,
        identity: str,
        channel: str,
        window: ResolvedWindow,
        limit: int,
        cursor: str | None,
        boundary: int | None = None,
    ) -> ProviderPage: ...

    async def thread(
        self,
        *,
        identity: str,
        channel: str,
        thread_id: str,
        window: ResolvedWindow,
        limit: int,
        cursor: str | None,
        boundary: int | None = None,
    ) -> ProviderPage: ...

    async def message(
        self, *, identity: str, channel: str, message_id: str
    ) -> ChannelMessage | None: ...


def reader_for(readers: Mapping[str, ChannelReader], kind: str) -> ChannelReader | None:
    reader = readers.get(kind)
    if reader is None or ChannelCapability.HISTORY_READ not in reader.capabilities:
        return None
    return reader
