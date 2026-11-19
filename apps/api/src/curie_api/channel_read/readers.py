"""Application selection of the frozen gateway read ports."""

from collections.abc import Mapping
from typing import Protocol

from channel_protocol import ChannelCapability
from curie_channel_gateway.ports.reads import CanvasReader as CanvasReaderPort
from curie_channel_gateway.ports.reads import ChannelReader as ChannelReaderPort
from curie_channel_gateway.read_models import (
    BindingRoute,
    CanvasDocument,
    CanvasFile,
    CanvasPage,
    ChannelMessage,
    ProviderPage,
    ResolvedWindow,
)


class ChannelReader(
    ChannelReaderPort[BindingRoute, ResolvedWindow, ProviderPage, ChannelMessage], Protocol
):
    pass


class CanvasReader(
    CanvasReaderPort[BindingRoute, CanvasFile, CanvasPage, CanvasDocument], Protocol
):
    pass


def reader_for(readers: Mapping[str, ChannelReader], kind: str) -> ChannelReader | None:
    reader = readers.get(kind)
    if reader is None or ChannelCapability.HISTORY_READ not in reader.capabilities:
        return None
    return reader


def canvas_reader_for(
    readers: Mapping[str, CanvasReader], kind: str, capability: ChannelCapability
) -> CanvasReader | None:
    reader = readers.get(kind)
    if reader is None or capability not in reader.capabilities:
        return None
    return reader
