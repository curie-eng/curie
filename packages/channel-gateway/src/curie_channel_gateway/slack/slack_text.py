"""Lossless text partitions for Slack's pinned edit safety limit."""

from __future__ import annotations

import re
import unicodedata
from dataclasses import dataclass

# Pinned from the measured edit rejection recorded in test_slack_sink.py.
SLACK_ANSWER_MAX_BYTES = 4000
# Slack section plain_text is limited to 3,000 characters. This smaller UTF-8
# byte budget also leaves room for provider-side encoding differences.
SLACK_VERBATIM_MAX_BYTES = 2800
_ATOM = re.compile(r"```|`[^`\n]*`|<[^>\n]*>|\*[^*\n]+\*|_[^_\n]+_|~[^~\n]+~")
_URL = re.compile(r"https?://[^\s<>\"]+")
_URL_TRAILING = ".,;:!?'"
_URL_CLOSERS = {")": "(", "]": "["}


@dataclass(frozen=True)
class AnswerPart:
    content: str
    prefix: str = ""
    suffix: str = ""

    @property
    def text(self) -> str:
        return self.prefix + self.content + self.suffix


def _url_spans(text: str) -> list[tuple[int, int]]:
    """Locate http(s) URLs, leaving trailing prose punctuation outside them."""
    spans: list[tuple[int, int]] = []
    for match in _URL.finditer(text):
        start, end = match.span()
        while end > start:
            last = text[end - 1]
            opener = _URL_CLOSERS.get(last)
            if last in _URL_TRAILING or (
                opener is not None and text.count(opener, start, end) < text.count(last, start, end)
            ):
                end -= 1
                continue
            break
        if end - start > len(match.group().split("://", 1)[0]) + 3:
            spans.append((start, end))
    return spans


def split_verbatim_answer(
    text: str, *, max_bytes: int = SLACK_VERBATIM_MAX_BYTES
) -> list[AnswerPart]:
    """Partition literal text without adding, removing or normalizing characters.

    The budget stays inside the 3,000-character limit Slack documents for a
    text object:
    https://docs.slack.dev/reference/block-kit/composition-objects/text-object/
    Count UTF-8 bytes more conservatively, without splitting a code point.
    Prefer a line break, then a space, in the later half of a partition, and
    never split inside a URL that fits in one partition (ADR 0250).
    """
    if not text:
        return []
    spans = [
        span
        for span in _url_spans(text)
        if len(text[span[0] : span[1]].encode("utf-8")) <= max_bytes
    ]
    parts: list[AnswerPart] = []
    start = 0
    while len(text[start:].encode("utf-8")) > max_bytes:
        end = start + len(text[start:].encode("utf-8")[:max_bytes].decode("utf-8", "ignore"))
        floor = start + (end - start) // 2
        line = text.rfind("\n", floor, end)
        space = text.rfind(" ", floor, end)
        if line >= 0:
            end = line + 1
        elif space >= 0:
            end = space + 1
        for url_start, url_end in spans:
            if url_start < end < url_end and url_start > start:
                end = url_start
                break
        parts.append(AnswerPart(text[start:end]))
        start = end
    parts.append(AnswerPart(text[start:]))
    return parts


def verbatim_rich_text(text: str) -> dict[str, object]:
    """One rich text section carrying `text` exactly, with URLs as links."""
    elements: list[dict[str, str]] = []
    cursor = 0
    for start, end in _url_spans(text):
        if start > cursor:
            elements.append({"type": "text", "text": text[cursor:start]})
        url = text[start:end]
        elements.append({"type": "link", "url": url, "text": url})
        cursor = end
    if cursor < len(text):
        elements.append({"type": "text", "text": text[cursor:]})
    return {
        "type": "rich_text",
        "elements": [{"type": "rich_text_section", "elements": elements}],
    }


def split_answer(text: str, *, max_bytes: int = SLACK_ANSWER_MAX_BYTES) -> list[AnswerPart]:
    """Keep every source character, reopening long code fences between messages.

    Prefer a line boundary, then whitespace; preserve complete short mrkdwn
    links and inline spans. A construct larger than the message budget must
    cross messages, but none of its content is discarded. Unicode code points,
    combining marks and joiner sequences are kept together when they fit.
    """
    if len(text.encode()) <= max_bytes:
        return [AnswerPart(text)]
    atoms = list(_ATOM.finditer(text))
    parts: list[AnswerPart] = []
    start = 0
    fenced = False
    while start < len(text):
        prefix = "```\n" if fenced else ""
        # Reserve closing scaffolding even before a chunk opens a fence.
        budget = max_bytes - len(prefix.encode()) - 4
        end = start + len(text[start:].encode()[:budget].decode("utf-8", "ignore"))
        code_point_end = end
        if end < len(text):
            line = text.rfind("\n", start, end)
            space = text.rfind(" ", start, end)
            boundary = line + 1 if line >= start else space + 1
            if boundary > start + (end - start) // 2:
                end = boundary
            for atom in atoms:
                if atom.start() < end < atom.end() and atom.start() > start:
                    end = atom.start()
                    break
            while end > start and (
                unicodedata.combining(text[end])
                or text[end] == "\u200d"
                or text[end - 1] == "\u200d"
                or "\ufe00" <= text[end] <= "\ufe0f"
                or "\U0001f3fb" <= text[end] <= "\U0001f3ff"
            ):
                end -= 1
            if end > start and "\U0001f1e6" <= text[end] <= "\U0001f1ff":
                previous = end - 1
                while previous >= start and "\U0001f1e6" <= text[previous] <= "\U0001f1ff":
                    previous -= 1
                if (end - previous - 1) % 2:
                    end -= 1
        if end == start:
            # A single grapheme can itself exceed the provider budget. Keep
            # every code point instead of making the answer undeliverable.
            end = code_point_end
        content = text[start:end]
        # Inline atoms containing backticks are not fence delimiters.
        for atom in atoms:
            if start <= atom.start() < end and atom.group() == "```":
                fenced = not fenced
        suffix = "\n```" if fenced and end < len(text) else ""
        parts.append(AnswerPart(content, prefix, suffix))
        start = end
    return parts
