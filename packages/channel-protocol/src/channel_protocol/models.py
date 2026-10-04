"""Versioned semantic messages rendered by Slack, the Curie TUI, and future channels."""

from enum import StrEnum
from typing import Annotated, Literal

from pydantic import BaseModel, ConfigDict, Field

MessageVersion = Literal["1.0"]
MESSAGE_VERSION: MessageVersion = "1.0"
_STRICT = ConfigDict(extra="forbid")


class ChannelCapability(StrEnum):
    INTERACTIVE_ACTIONS = "interactive-actions"
    LIVE_STEERING = "live-steering"
    STREAMING = "streaming"
    RICH_CARDS = "rich-cards"
    THREADING = "threading"
    FILE_ATTACHMENTS = "file-attachments"
    HISTORY_READ = "history-read"


class ChannelCapabilities(BaseModel):
    model_config = _STRICT

    version: MessageVersion
    capabilities: list[ChannelCapability] = Field(default_factory=list)


class Action(BaseModel):
    """A semantic response the user may select; adapters choose its widget."""

    model_config = _STRICT

    label: str = Field(min_length=1, max_length=75)
    value: str = Field(min_length=1, max_length=255)


class MessageField(BaseModel):
    model_config = _STRICT

    label: str
    value: str


class MessageLink(BaseModel):
    model_config = _STRICT

    label: str
    url: str


class ChoiceIntent(BaseModel):
    model_config = _STRICT

    kind: Literal["choice"]
    id: str = Field(min_length=1, max_length=255)
    prompt: str | None = None
    options: list[Action] = Field(min_length=1, max_length=10)
    allow_free_text: bool = True


class ConfirmIntent(BaseModel):
    model_config = _STRICT

    kind: Literal["confirm"]
    id: str = Field(min_length=1, max_length=255)
    prompt: str
    confirm: Action
    cancel: Action
    allow_free_text: bool = False


InteractionIntent = Annotated[ChoiceIntent | ConfirmIntent, Field(discriminator="kind")]


class OutboundMessage(BaseModel):
    """One adapter-neutral reply with a mandatory complete text fallback."""

    model_config = _STRICT

    version: MessageVersion
    text: str
    status: str | None = None
    header: str | None = None
    fields: list[MessageField] = Field(default_factory=list)
    links: list[MessageLink] = Field(default_factory=list)
    footer: str | None = None
    interaction: InteractionIntent | None = None
