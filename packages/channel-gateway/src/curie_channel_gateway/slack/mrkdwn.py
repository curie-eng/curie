"""Convert Markdown to Slack ``mrkdwn`` before editing a message in place.

The runner emits standard Markdown, but Slack's ``chat.update`` renders its own
``mrkdwn`` dialect, not Markdown: bold is a single ``*`` (not ``**``), links are
``<url|text>`` (not ``[text](url)``), and there are no headings. Left unconverted
the reply shows raw ``**bold**`` and ``[text](url)`` to the user.

``to_mrkdwn`` is a small pure function: it rewrites bold, links, and headings and
leaves everything else (bullets, italics, inline code, fenced code) untouched.
Code spans and fenced blocks are protected -- Markdown syntax inside them is
copied verbatim so a code sample never gets rewritten.
"""

from __future__ import annotations

import re

# Fenced code blocks (```...```) are copied verbatim. Captured so re.split keeps
# the fences in the output stream.
_FENCE_RE = re.compile(r"(```.*?```)", re.DOTALL)
# Inline code spans (`...`) within a non-fenced segment, likewise protected.
_INLINE_CODE_RE = re.compile(r"(`[^`]*`)")
# **bold** -> *bold* (Slack bold is a single asterisk).
_BOLD_RE = re.compile(r"\*\*(.+?)\*\*", re.DOTALL)
# [text](url) -> <url|text>.
_LINK_RE = re.compile(r"\[([^\]]+)\]\(([^)]+)\)")
# ATX headings (# .. ######) -> a bold line; Slack has no heading syntax.
_HEADING_RE = re.compile(r"^[ ]{0,3}#{1,6}[ \t]+(.*?)[ \t]*#*[ \t]*$", re.MULTILINE)
# List bullets (- item / * item) -> the bullet character; Slack mrkdwn has no
# list syntax. Leading whitespace is preserved so nested bullets stay indented.
# The trailing space is required, which excludes *italic* (no space after *).
_BULLET_RE = re.compile(r"^([ \t]*)[-*] ", re.MULTILINE)


def _convert_segment(text: str) -> str:
    """Rewrite the Markdown constructs Slack renders differently."""

    text = _HEADING_RE.sub(lambda m: f"*{m.group(1).strip()}*", text)
    text = _BULLET_RE.sub(lambda m: f"{m.group(1)}• ", text)
    text = _BOLD_RE.sub(lambda m: f"*{m.group(1)}*", text)
    text = _LINK_RE.sub(lambda m: f"<{m.group(2)}|{m.group(1)}>", text)
    return text


def to_mrkdwn(text: str) -> str:
    """Convert Markdown to Slack mrkdwn, leaving code spans/fences untouched."""

    if not text:
        return text
    out: list[str] = []
    for fence_part in _FENCE_RE.split(text):
        if fence_part.startswith("```"):
            out.append(fence_part)
            continue
        for code_part in _INLINE_CODE_RE.split(fence_part):
            if len(code_part) >= 2 and code_part.startswith("`") and code_part.endswith("`"):
                out.append(code_part)
            else:
                out.append(_convert_segment(code_part))
    return "".join(out)
