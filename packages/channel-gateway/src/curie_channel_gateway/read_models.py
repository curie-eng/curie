"""Read result records shared by provider ports and their application consumers."""

from dataclasses import dataclass
from datetime import UTC, datetime, timedelta


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


@dataclass(frozen=True)
class CanvasFile:
    """What the provider's metadata says about one canvas: where it is shared,
    whether it is a canvas at all, and where its content is fetched from."""

    id: str
    title: str
    created: int
    shared_in: frozenset[str]
    is_canvas: bool
    url: str


@dataclass(frozen=True)
class CanvasSummaryRecord:
    id: str
    title: str
    created: int


@dataclass(frozen=True)
class CanvasCellRecord:
    section_id: str | None
    text: str
    truncated: bool


@dataclass(frozen=True)
class CanvasTableRecord:
    header: list[CanvasCellRecord]
    rows: list[list[CanvasCellRecord]]


@dataclass(frozen=True)
class CanvasParagraphRecord:
    section_id: str | None
    text: str
    truncated: bool


@dataclass(frozen=True)
class CanvasDocument:
    tables: list[CanvasTableRecord]
    paragraphs: list[CanvasParagraphRecord]


@dataclass(frozen=True)
class CanvasPage:
    canvases: list[CanvasSummaryRecord]
    has_more: bool


@dataclass(frozen=True)
class ResolvedWindow:
    oldest: datetime
    latest: datetime


def epoch_micros(moment: datetime) -> int:
    delta = moment - datetime(1970, 1, 1, tzinfo=UTC)
    return (delta.days * 86_400 + delta.seconds) * 1_000_000 + delta.microseconds


def from_epoch_micros(value: int) -> datetime:
    return datetime(1970, 1, 1, tzinfo=UTC) + timedelta(microseconds=value)
