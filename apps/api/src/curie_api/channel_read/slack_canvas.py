"""Slack canvas list, read and cell edit, and nothing else (ADR 0200, #3819).

The only place the API reads or writes a Slack canvas. The caller has already
authorized the operation and, for a canvas, decides whether the place Slack
reports it shared is one of the agent's bindings; nothing here selects a
channel. Nothing here logs a token, a title or any canvas text.

Methods and shapes:
https://docs.slack.dev/reference/methods/files.info (the canvas metadata: where
it is shared, whether it is a canvas, its ``url_private``)
https://docs.slack.dev/reference/methods/files.list (``types=canvas``,
https://docs.slack.dev/surfaces/canvases/#finding-canvases-with-fileslist)
https://docs.slack.dev/reference/methods/conversations.info (``is_member``)
https://docs.slack.dev/reference/methods/canvases.edit (one ``replace`` change)
https://docs.slack.dev/reference/methods/canvases.sections.lookup is not the read
path: it returns section ids only, no text, so a read downloads the canvas HTML.
https://docs.slack.dev/apis/web-api/rate-limits
"""

from __future__ import annotations

import json
import re
import unicodedata
from collections.abc import Mapping
from dataclasses import dataclass, field
from html.parser import HTMLParser
from typing import Any

import httpx
from channel_protocol import ChannelCapability

from .errors import ChannelReadRefused
from .readers import (
    BindingRoute,
    CanvasCellRecord,
    CanvasDocument,
    CanvasFile,
    CanvasPage,
    CanvasParagraphRecord,
    CanvasSummaryRecord,
    CanvasTableRecord,
)
from .slack_reads import SLACK_API, TEXT_LIMIT, TRUNCATION_MARKER, SlackChannelReader

FILES_INFO = "files.info"
FILES_LIST = "files.list"
CONVERSATIONS_INFO = "conversations.info"
CANVASES_EDIT = "canvases.edit"
# The ``url_private`` GET; a cooldown name, not a Web API method.
FILES_DOWNLOAD = "files.download"

CANVAS_FILETYPE = "quip"
CANVAS_MIMETYPE = "application/vnd.slack-docs"
CANVAS_ROOT_CLASS = "quip-canvas-content"
DOWNLOAD_HOST = "files.slack.com"
MAX_DOWNLOAD_BYTES = 1024 * 1024
MAX_CELL_TEXT = 1000
LIST_COUNT = 100

_CANVAS_ID = re.compile(r"^F[A-Z0-9]{8,24}$")
_SECTION_ID = re.compile(r"^temp:C:[A-Za-z0-9]{8,64}$")
_NOT_MEMBER = frozenset({"not_in_channel", "channel_not_found"})
_NOT_FOUND = frozenset({"file_not_found", "file_deleted", "canvas_not_found"})
_EDIT_REFUSED = frozenset({"restricted_action", "not_allowed", "access_denied", "canvas_disabled"})

# The edit text rule: plain text a markdown edit cannot reinterpret.
_MARKDOWN_CHARACTERS = frozenset("|`*_~[]<>\\")
_EMOJI_SHORTCODE = re.compile(r":[A-Za-z0-9_+'-]+:")
_CHARACTER_REFERENCE = re.compile(r"&(#[0-9]+|#[xX][0-9A-Fa-f]+|[A-Za-z][A-Za-z0-9]*);")
_LIST_MARKER = re.compile(r"^\d+[.)]\s")
_BLOCK_LEADERS = ("#", "-", "+", ">")


def valid_canvas_id(value: str) -> bool:
    return bool(_CANVAS_ID.match(value))


def valid_section_id(value: str) -> bool:
    return bool(_SECTION_ID.match(value))


def cell_text_problem(text: str) -> str | None:
    """Why a cell edit's text is refused, or None. Never quotes the text."""

    if not text:
        return "the cell text is empty"
    if len(text) > MAX_CELL_TEXT:
        return f"the cell text is longer than {MAX_CELL_TEXT} characters"
    if text != text.strip():
        return "the cell text has leading or trailing whitespace"
    if any(unicodedata.category(ch) == "Cc" for ch in text):
        return "the cell text has a control character, a line break or a tab"
    if any(ch in _MARKDOWN_CHARACTERS for ch in text):
        return "the cell text has a markdown character: | ` * _ ~ [ ] < > or a backslash"
    if _EMOJI_SHORTCODE.search(text):
        return "the cell text has an emoji shortcode"
    if _CHARACTER_REFERENCE.search(text):
        return "the cell text has an HTML entity or character reference"
    if text.startswith(_BLOCK_LEADERS) or _LIST_MARKER.match(text):
        return "the cell text starts like a heading, list or quote"
    return None


def _truncate(text: str) -> tuple[str, bool]:
    if len(text) > TEXT_LIMIT:
        return text[:TEXT_LIMIT] + TRUNCATION_MARKER, True
    return text, False


# -- The canvas HTML -------------------------------------------------------- #
_BLOCKS = frozenset({"h1", "h2", "h3", "h4", "h5", "h6", "p", "li", "pre", "blockquote"})
_CELLS = frozenset({"td", "th"})


@dataclass
class _Cell:
    paragraphs: list[tuple[str | None, list[str]]] = field(default_factory=list)
    loose: list[str] = field(default_factory=list)
    nested: bool = False

    def record(self) -> CanvasCellRecord:
        parts = ["".join(chunks) for _, chunks in self.paragraphs]
        loose = "".join(self.loose)
        if loose.strip():
            parts.append(loose)
        text, truncated = _truncate("\n".join(parts))
        section_id = None
        if (
            len(self.paragraphs) == 1
            and not self.nested
            and not loose.strip()
            and not truncated
            and (sid := self.paragraphs[0][0]) is not None
            and valid_section_id(sid)
        ):
            section_id = sid
        return CanvasCellRecord(section_id=section_id, text=text, truncated=truncated)


@dataclass
class _Table:
    rows: list[list[_Cell]] = field(default_factory=list)


class _CanvasParser(HTMLParser):
    """Tables by position and the text outside them, under the canvas root."""

    def __init__(self) -> None:
        super().__init__(convert_charrefs=True)
        self.rooted = False
        self.tables: list[_Table] = []
        self.paragraphs: list[tuple[str | None, list[str]]] = []
        self._depth = 0  # table nesting
        self._table: _Table | None = None
        self._cell: _Cell | None = None
        self._cell_paragraph: list[str] | None = None
        self._block: list[str] | None = None
        self._block_depth = 0

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        values = dict(attrs)
        if not self.rooted:
            if CANVAS_ROOT_CLASS in (values.get("class") or "").split():
                self.rooted = True
            return
        if tag == "br":
            self.handle_data(" ")
            return
        if tag == "table":
            self._depth += 1
            if self._depth == 1:
                self._table = _Table()
                self.tables.append(self._table)
            elif self._cell is not None:
                self._cell.nested = True
            return
        if self._depth == 1 and self._table is not None:
            if tag == "tr":
                self._table.rows.append([])
                self._cell = None
            elif tag in _CELLS:
                if not self._table.rows:
                    self._table.rows.append([])
                self._cell = _Cell()
                self._table.rows[-1].append(self._cell)
                self._cell_paragraph = None
            elif tag == "p" and self._cell is not None:
                self._cell_paragraph = []
                self._cell.paragraphs.append((values.get("id"), self._cell_paragraph))
            return
        if self._depth > 1:
            # A nested table's paragraphs become more paragraphs of the outer cell.
            if tag == "p" and self._cell is not None:
                self._cell_paragraph = []
                self._cell.paragraphs.append((None, self._cell_paragraph))
            elif tag in _CELLS and self._cell is not None:
                self._cell_paragraph = None
            return
        if tag in _BLOCKS:
            if self._block is None:
                self._block = []
                self.paragraphs.append((values.get("id"), self._block))
                self._block_depth = 1
            else:
                self._block_depth += 1

    def handle_startendtag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        self.handle_starttag(tag, attrs)
        if tag not in ("br",):
            self.handle_endtag(tag)

    def handle_endtag(self, tag: str) -> None:
        if not self.rooted:
            return
        if tag == "table":
            if self._depth > 0:
                self._depth -= 1
            if self._depth == 0:
                self._table = None
                self._cell = None
                self._cell_paragraph = None
            return
        if self._depth >= 1:
            if tag == "p":
                self._cell_paragraph = None
            elif tag in _CELLS and self._depth == 1:
                self._cell = None
                self._cell_paragraph = None
            return
        if tag in _BLOCKS and self._block is not None:
            self._block_depth -= 1
            if self._block_depth <= 0:
                self._block = None

    def handle_data(self, data: str) -> None:
        if not self.rooted:
            return
        if self._depth >= 1:
            if self._cell is None:
                return
            if self._cell_paragraph is not None:
                self._cell_paragraph.append(data)
            else:
                self._cell.loose.append(data)
            return
        if self._block is not None:
            self._block.append(data)


def _unreadable() -> ChannelReadRefused:
    return ChannelReadRefused(502, "canvas_unreadable", "Slack did not serve the canvas content")


def parse_canvas_html(html: str) -> CanvasDocument:
    """Every top level table as a header row and rows, and the text outside them."""

    parser = _CanvasParser()
    parser.feed(html)
    parser.close()
    if not parser.rooted:
        raise _unreadable()
    tables = [
        CanvasTableRecord(
            header=[cell.record() for cell in table.rows[0]] if table.rows else [],
            rows=[[cell.record() for cell in row] for row in table.rows[1:]],
        )
        for table in parser.tables
    ]
    paragraphs = []
    for block_id, chunks in parser.paragraphs:
        text, truncated = _truncate("".join(chunks))
        section_id = (
            block_id
            if block_id is not None and valid_section_id(block_id) and not truncated
            else None
        )
        paragraphs.append(
            CanvasParagraphRecord(section_id=section_id, text=text, truncated=truncated)
        )
    return CanvasDocument(tables=tables, paragraphs=paragraphs)


# -- Slack calls -------------------------------------------------------------- #
def _provider_error() -> ChannelReadRefused:
    return ChannelReadRefused(502, "provider_error", "Slack could not answer the canvas call")


def _outcome_unknown() -> ChannelReadRefused:
    return ChannelReadRefused(
        502, "edit_outcome_unknown", "Slack did not confirm the edit; it may have applied"
    )


def _rate_limited(response: httpx.Response) -> ChannelReadRefused:
    try:
        retry_after: int | None = max(0, int(response.headers["Retry-After"]))
    except (KeyError, ValueError):
        retry_after = None
    return ChannelReadRefused(
        429,
        "provider_rate_limited",
        "Slack is rate limiting canvas calls; retry later",
        retry_after=retry_after,
    )


def _refusal_for(
    method: str, body: Mapping[str, Any], response: httpx.Response
) -> ChannelReadRefused:
    """The named refusal for a definite ``ok: false`` answer."""

    error = body.get("error")
    if error == "ratelimited":
        return _rate_limited(response)
    if error == "missing_scope":
        return ChannelReadRefused(
            503, "provider_scope_missing", "the Slack app lacks a scope this canvas call needs"
        )
    if error in _NOT_FOUND:
        return ChannelReadRefused(404, "canvas_not_found", "Slack has no such canvas")
    if error in _NOT_MEMBER and method in (CONVERSATIONS_INFO, FILES_LIST):
        return ChannelReadRefused(
            403, "not_member", "the Slack app is not a member of this channel"
        )
    if method == CANVASES_EDIT:
        detail = body.get("detail")
        if (
            error == "canvas_editing_failed"
            and isinstance(detail, str)
            and detail.startswith("Section ")
        ):
            return ChannelReadRefused(
                409, "section_not_found", "the canvas no longer has that section"
            )
        if error in _EDIT_REFUSED:
            return ChannelReadRefused(403, "canvas_edit_refused", "Slack refused the canvas edit")
    return _provider_error()


def _download_url_ok(url: str) -> bool:
    try:
        parsed = httpx.URL(url)
    except (httpx.InvalidURL, TypeError, ValueError):
        return False
    return (
        parsed.scheme == "https"
        and parsed.host == DOWNLOAD_HOST
        and parsed.port is None
        and not parsed.userinfo
        and url.startswith(f"https://{DOWNLOAD_HOST}/")
    )


def _text_field(value: object) -> str:
    return value if isinstance(value, str) else ""


def _int_field(value: object) -> int:
    return value if isinstance(value, int) and not isinstance(value, bool) else 0


class SlackCanvasReader:
    capabilities = frozenset({ChannelCapability.CANVAS_READ, ChannelCapability.CANVAS_EDIT})

    def __init__(self, http: httpx.AsyncClient, tokens: Mapping[str, str]) -> None:
        self._http = http
        self._tokens = tokens
        self._identities = SlackChannelReader(http, tokens)

    def identity(self, routes: list[BindingRoute]) -> str:
        return self._identities.identity(routes)

    def has_identity(self, identity: str) -> bool:
        return self._identities.has_identity(identity)

    def identity_key(self, identity: str) -> str:
        return self._identities.identity_key(identity)

    def valid_canvas_id(self, value: str) -> bool:
        return valid_canvas_id(value)

    def valid_section_id(self, value: str) -> bool:
        return valid_section_id(value)

    def _token(self, identity: str) -> str:
        token = self._tokens.get(identity)
        if not token:
            raise ChannelReadRefused(
                503, "provider_unconfigured", "no Slack credential serves this binding"
            )
        return token

    async def _call(self, identity: str, method: str, params: Mapping[str, str]) -> dict[str, Any]:
        token = self._token(identity)
        try:
            response = await self._http.get(
                SLACK_API + method,
                params=dict(params),
                headers={"Authorization": f"Bearer {token}"},
            )
        except httpx.HTTPError:
            raise _provider_error() from None
        if response.status_code == 429:
            raise _rate_limited(response)
        if response.status_code != 200:
            raise _provider_error()
        try:
            body = response.json()
        except ValueError:
            raise _provider_error() from None
        if not isinstance(body, dict):
            raise _provider_error()
        if body.get("ok") is True:
            return body
        raise _refusal_for(method, body, response)

    async def locate(self, *, identity: str, canvas_id: str) -> CanvasFile | None:
        try:
            body = await self._call(identity, FILES_INFO, {"file": canvas_id})
        except ChannelReadRefused as refused:
            if refused.code == "channel_read.canvas_not_found":
                return None
            raise
        raw = body.get("file")
        if not isinstance(raw, dict):
            raise _provider_error()
        if raw.get("id") != canvas_id:
            return None
        shared: set[str] = set()
        for key in ("channels", "groups"):
            places = raw.get(key)
            if isinstance(places, list):
                shared.update(p for p in places if isinstance(p, str))
        return CanvasFile(
            id=canvas_id,
            title=_text_field(raw.get("title")),
            created=_int_field(raw.get("created")),
            shared_in=frozenset(shared),
            is_canvas=raw.get("filetype") == CANVAS_FILETYPE
            and raw.get("mimetype") == CANVAS_MIMETYPE,
            url=_text_field(raw.get("url_private")),
        )

    async def is_member(self, *, identity: str, channel: str) -> bool:
        body = await self._call(identity, CONVERSATIONS_INFO, {"channel": channel})
        raw = body.get("channel")
        if not isinstance(raw, dict):
            raise _provider_error()
        return raw.get("is_member") is True

    async def list(self, *, identity: str, channel: str) -> CanvasPage:
        body = await self._call(
            identity,
            FILES_LIST,
            {"channel": channel, "types": "canvas", "count": str(LIST_COUNT)},
        )
        files = body.get("files")
        if not isinstance(files, list):
            raise _provider_error()
        canvases = [
            CanvasSummaryRecord(
                id=item["id"],
                title=_text_field(item.get("title")),
                created=_int_field(item.get("created")),
            )
            for item in files
            if isinstance(item, dict)
            and isinstance(item.get("id"), str)
            and valid_canvas_id(item["id"])
            and item.get("filetype") == CANVAS_FILETYPE
            and item.get("mimetype") == CANVAS_MIMETYPE
        ]
        paging = body.get("paging")
        has_more = False
        if isinstance(paging, dict):
            page, pages = paging.get("page"), paging.get("pages")
            has_more = isinstance(page, int) and isinstance(pages, int) and pages > page
        return CanvasPage(canvases=canvases, has_more=has_more)

    async def document(self, *, identity: str, file: CanvasFile) -> CanvasDocument:
        # The bearer token goes to files.slack.com over https and nowhere else.
        if not _download_url_ok(file.url):
            raise _unreadable()
        token = self._token(identity)
        chunks: list[bytes] = []
        size = 0
        try:
            async with self._http.stream(
                "GET",
                file.url,
                headers={"Authorization": f"Bearer {token}"},
                follow_redirects=False,
            ) as response:
                if response.status_code == 429:
                    raise _rate_limited(response)
                if response.status_code != 200:
                    raise _unreadable()
                async for chunk in response.aiter_bytes():
                    size += len(chunk)
                    if size > MAX_DOWNLOAD_BYTES:
                        raise ChannelReadRefused(
                            409, "canvas_too_large", "the canvas is larger than one mebibyte"
                        )
                    chunks.append(chunk)
        except httpx.HTTPError:
            raise _unreadable() from None
        return parse_canvas_html(b"".join(chunks).decode("utf-8", errors="replace"))

    async def replace_cell(
        self, *, identity: str, canvas_id: str, section_id: str, text: str
    ) -> None:
        """Replace one section's text. A definite refusal is its named code; an
        answer that does not say whether Slack applied it is ``edit_outcome_unknown``."""

        token = self._token(identity)
        payload = {
            "canvas_id": canvas_id,
            "changes": [
                {
                    "operation": "replace",
                    "section_id": section_id,
                    "document_content": {"type": "markdown", "markdown": text},
                }
            ],
        }
        try:
            response = await self._http.post(
                SLACK_API + CANVASES_EDIT,
                content=json.dumps(payload).encode(),
                headers={
                    "Authorization": f"Bearer {token}",
                    "Content-Type": "application/json; charset=utf-8",
                },
            )
        except httpx.HTTPError:
            raise _outcome_unknown() from None
        if response.status_code == 429:
            raise _rate_limited(response)
        if response.status_code >= 500:
            raise _outcome_unknown()
        if response.status_code != 200:
            raise _provider_error()
        try:
            body = response.json()
        except ValueError:
            raise _outcome_unknown() from None
        if not isinstance(body, dict) or not isinstance(body.get("ok"), bool):
            raise _outcome_unknown()
        if body["ok"] is True:
            return
        raise _refusal_for(CANVASES_EDIT, body, response)
