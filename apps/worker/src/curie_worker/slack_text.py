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


@dataclass(frozen=True)
class AnswerPart:
    content: str
    prefix: str = ""
    suffix: str = ""

    @property
    def text(self) -> str:
        return self.prefix + self.content + self.suffix


def split_verbatim_answer(text: str) -> list[AnswerPart]:
    """Partition literal text without adding, removing or normalizing characters.

    The budget stays inside the 3,000-character limit Slack documents for a
    text object:
    https://docs.slack.dev/reference/block-kit/composition-objects/text-object/
    Count UTF-8 bytes more conservatively, without splitting a code point.
    """
    if not text:
        return []
    parts: list[AnswerPart] = []
    start = 0
    size = 0
    for index, char in enumerate(text):
        encoded_size = len(char.encode("utf-8"))
        if size + encoded_size > SLACK_VERBATIM_MAX_BYTES:
            parts.append(AnswerPart(text[start:index]))
            start = index
            size = 0
        size += encoded_size
    parts.append(AnswerPart(text[start:]))
    return parts


def split_answer(text: str) -> list[AnswerPart]:
    """Keep every source character, reopening long code fences between messages.

    Prefer a line boundary, then whitespace; preserve complete short mrkdwn
    links and inline spans. A construct larger than the message budget must
    cross messages, but none of its content is discarded. Unicode code points,
    combining marks and joiner sequences are kept together when they fit.
    """
    if len(text.encode()) <= SLACK_ANSWER_MAX_BYTES:
        return [AnswerPart(text)]
    atoms = list(_ATOM.finditer(text))
    parts: list[AnswerPart] = []
    start = 0
    fenced = False
    while start < len(text):
        prefix = "```\n" if fenced else ""
        # Reserve closing scaffolding even before a chunk opens a fence.
        budget = SLACK_ANSWER_MAX_BYTES - len(prefix.encode()) - 4
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
