"""Wire shapes of the channel read routes (ADR 0100, #2877).

Times stay strings in the request so a malformed one is refused with the named
``channel_read.window_invalid`` rather than a bare validation error.
"""

import uuid
from typing import Literal, Self

from pydantic import BaseModel, ConfigDict, Field, model_validator


class ChannelSelector(BaseModel):
    """A bound channel named by the agent. ``kind`` is required in practice and
    refused by name when absent, so the agent learns what to send."""

    model_config = ConfigDict(extra="forbid")

    kind: str | None = Field(default=None, min_length=1, max_length=64)
    address: str = Field(min_length=1, max_length=255)


class ChannelAddress(BaseModel):
    model_config = ConfigDict(extra="forbid")

    kind: str = Field(min_length=1, max_length=64)
    address: str = Field(min_length=1, max_length=255)


class ChannelReadRequest(BaseModel):
    """One page of history, one thread, or one message of a bound channel."""

    model_config = ConfigDict(extra="forbid")

    operation: Literal["history", "thread", "message"] | None = None
    channel: ChannelSelector | None = None
    oldest: str | None = Field(default=None, max_length=64)
    latest: str | None = Field(default=None, max_length=64)
    thread_id: str | None = Field(default=None, max_length=64)
    message_id: str | None = Field(default=None, max_length=64)
    cursor: str | None = Field(default=None, max_length=4096)
    limit: int | None = None


class ChannelReadMessage(BaseModel):
    """A channel neutral record. ``id`` is opaque: ``<ts>`` for a parent or an
    unthreaded message, ``<thread_ts>:<ts>`` for a thread reply."""

    id: str
    thread_id: str | None = None
    timestamp: str
    author: str
    text: str
    truncated: bool
    provenance: str
    reply_count: int | None = None


class ChannelReadPage(BaseModel):
    messages: list[ChannelReadMessage]
    has_more: bool
    next_cursor: str | None = None


class ChannelReadContextMint(BaseModel):
    """The worker's request for one logical turn's capability."""

    model_config = ConfigDict(extra="forbid")

    agent_id: uuid.UUID
    deployment_id: uuid.UUID
    event_id: str = Field(min_length=1, max_length=512)
    mode: Literal["open", "steer"]
    owner: str | None = Field(
        default=None, min_length=16, max_length=64, pattern=r"^[A-Za-z0-9_-]+$"
    )
    default_channel: ChannelAddress | None = None
    ttl_s: int = Field(ge=1, le=86400)

    @model_validator(mode="after")
    def _owner_matches_mode(self) -> Self:
        if self.mode == "open" and self.owner is None:
            raise ValueError("an open mint names its owner")
        if self.mode == "steer" and self.owner is not None:
            raise ValueError("a steer mint keeps the opener's owner and names none")
        return self


class ChannelReadContext(BaseModel):
    """The capability and the logical turn digest the worker revokes by."""

    token: str
    generation: int
    expires_at: int
    turn_key: str
