"""Atlassian Document Format to markdown and back (#4358 decisions 14 and 15).

Jira Cloud's REST v3 carries every description and comment body as ADF
(https://developer.atlassian.com/cloud/jira/platform/apis/document/structure/).
`to_markdown` reads a ticket for the sandbox: it covers the nodes and marks
decision 14 names, renders media as ``[attachment: <alt or id>]``, and renders
any other node as what its children render, so unknown content keeps its text.
`from_markdown` writes the core's status body: headings, paragraphs, bullet
and ordered lists, fenced code, block quotes, rules, links and the strong, em
and code marks (decision 15). A checklist line keeps its ``[x] `` or ``[ ] ``
text, an image becomes a link, and HTML comments are dropped, so the core's
marker line never becomes visible text (the marker lives in a comment entity
property instead, `curie_api.forges.jira_cloud.comments`).
"""

from __future__ import annotations

import re
from collections.abc import Mapping, Sequence
from html import unescape
from html.entities import html5
from typing import Any

# ADF -> markdown -----------------------------------------------------------------

# Nodes that sit inside a paragraph. Any other node is a block.
# https://developer.atlassian.com/cloud/jira/platform/apis/document/nodes/
_INLINE = frozenset(
    {"text", "hardBreak", "mention", "emoji", "inlineCard", "status", "date", "placeholder"}
)


def to_markdown(node: Mapping[str, Any] | None) -> str:
    """``node`` (normally a ``doc``) as markdown, blocks separated by a blank line."""

    if not isinstance(node, Mapping):
        return ""
    if node.get("type") in _INLINE:
        return _inline([node])
    if node.get("type") == "doc":
        return "\n\n".join(_blocks(_children(node)))
    return "\n\n".join(_block(node))


def _children(node: Mapping[str, Any]) -> list[Mapping[str, Any]]:
    content = node.get("content")
    if not isinstance(content, list):
        return []
    return [child for child in content if isinstance(child, Mapping)]


def _attrs(node: Mapping[str, Any]) -> Mapping[str, Any]:
    attrs = node.get("attrs")
    return attrs if isinstance(attrs, Mapping) else {}


def _blocks(nodes: Sequence[Mapping[str, Any]]) -> list[str]:
    """Each block's markdown; a run of inline nodes reads as one paragraph."""

    rendered: list[str] = []
    run: list[Mapping[str, Any]] = []
    for node in nodes:
        if node.get("type") in _INLINE:
            run.append(node)
            continue
        if run:
            rendered.append(_inline(run))
            run = []
        rendered.extend(_block(node))
    if run:
        rendered.append(_inline(run))
    return [block for block in rendered if block]


def _block(node: Mapping[str, Any]) -> list[str]:
    kind = node.get("type")
    children = _children(node)
    if kind == "paragraph":
        return [_inline(children)]
    if kind == "heading":
        level = _attrs(node).get("level")
        level = level if isinstance(level, int) and 1 <= level <= 6 else 1
        return [f"{'#' * level} {_inline(children)}"]
    if kind == "bulletList":
        return [_list(children, ordered=False, start=1)]
    if kind == "orderedList":
        order = _attrs(node).get("order")
        return [_list(children, ordered=True, start=order if isinstance(order, int) else 1)]
    if kind == "codeBlock":
        language = _attrs(node).get("language")
        fence = f"```{language if isinstance(language, str) else ''}"
        return [f"{fence}\n{_plain(children)}\n```"]
    if kind in {"blockquote", "panel"}:
        return [_quote("\n\n".join(_blocks(children)))]
    if kind == "rule":
        return ["---"]
    if kind == "table":
        return [_table(children)]
    if kind == "media":
        attrs = _attrs(node)
        alt = attrs.get("alt")
        name = alt if isinstance(alt, str) and alt else attrs.get("id", "")
        return [f"[attachment: {name}]"]
    # mediaSingle, mediaGroup, expand, layouts and any node decision 14 does
    # not name: what its children render.
    return _blocks(children)


def _list(items: Sequence[Mapping[str, Any]], *, ordered: bool, start: int) -> str:
    lines: list[str] = []
    for index, item in enumerate(items):
        marker = f"{start + index}. " if ordered else "- "
        body = "\n".join(_blocks(_children(item)))
        first, *rest = body.split("\n") if body else [""]
        lines.append(f"{marker}{first}")
        lines.extend(f"{' ' * len(marker)}{line}" if line else "" for line in rest)
    return "\n".join(lines)


def _quote(text: str) -> str:
    return "\n".join(f"> {line}" if line else ">" for line in text.split("\n"))


def _table(rows: Sequence[Mapping[str, Any]]) -> str:
    rendered: list[list[str]] = []
    for row in rows:
        cells = [
            " ".join(_blocks(_children(cell))).replace("\n", " ").replace("|", "\\|")
            for cell in _children(row)
        ]
        rendered.append(cells)
    if not rendered:
        return ""
    width = max(len(row) for row in rendered)
    lines = [_row(rendered[0], width), _row(["---"] * width, width)]
    lines.extend(_row(row, width) for row in rendered[1:])
    return "\n".join(lines)


def _row(cells: Sequence[str], width: int) -> str:
    padded = [*cells, *([""] * (width - len(cells)))]
    return "| " + " | ".join(padded) + " |"


def _plain(nodes: Sequence[Mapping[str, Any]]) -> str:
    """Text content only, for code blocks."""

    return "".join(
        str(node.get("text", "")) if node.get("type") == "text" else _plain(_children(node))
        for node in nodes
    )


def _inline(nodes: Sequence[Mapping[str, Any]]) -> str:
    parts: list[str] = []
    for node in nodes:
        kind = node.get("type")
        attrs = _attrs(node)
        if kind == "text":
            parts.append(_marked(str(node.get("text", "")), node.get("marks")))
        elif kind == "hardBreak":
            parts.append("\n")
        elif kind == "mention":
            text = attrs.get("text")
            name = text.removeprefix("@") if isinstance(text, str) and text else attrs.get("id")
            parts.append(f"@{name}")
        elif kind == "inlineCard":
            parts.append(str(attrs.get("url", "")))
        elif _children(node):
            parts.append(_inline(_children(node)))
        else:
            text = attrs.get("text")
            parts.append(text if isinstance(text, str) else "")
    return "".join(parts)


def _marked(text: str, marks: Any) -> str:
    names: dict[str, Mapping[str, Any]] = {}
    if isinstance(marks, list):
        for mark in marks:
            if isinstance(mark, Mapping) and isinstance(mark.get("type"), str):
                names[mark["type"]] = _attrs(mark)
    # Innermost first, so a linked bold word reads [**word**](href). A mark
    # decision 14 does not name (text color, underline) leaves plain text.
    if "code" in names:
        text = f"`{text}`"
    if "em" in names:
        text = f"*{text}*"
    if "strong" in names:
        text = f"**{text}**"
    if "link" in names:
        href = names["link"].get("href")
        if isinstance(href, str) and href:
            text = f"[{text}]({href})"
    return text


# markdown -> ADF -------------------------------------------------------------------

_COMMENT = re.compile(r"<!--.*?-->", re.DOTALL)
_FENCE = re.compile(r"^\s*(```+|~~~+)\s*([\w+.#-]*)\s*$")
_HEADING = re.compile(r"^(#{1,6})\s+(.*?)\s*#*\s*$")
_RULE = re.compile(r"^\s*(?:-\s*){3,}$|^\s*(?:\*\s*){3,}$|^\s*(?:_\s*){3,}$")
_BULLET = re.compile(r"^(\s*)[-*+]\s+(.*)$")
_ORDERED = re.compile(r"^(\s*)(\d{1,9})[.)]\s+(.*)$")
_QUOTE = re.compile(r"^\s*>\s?(.*)$")
_ENTITY_REFERENCE = re.compile(r"&(?:#[0-9]{1,7}|#[xX][0-9A-Fa-f]{1,6}|[A-Za-z][A-Za-z0-9]{1,31});")
_SPAN = re.compile(
    r"\\(?P<escaped>[!-/:-@\[-`{-~])"
    r"|(?P<code_fence>`+)(?P<code>.+?)(?P=code_fence)"
    r"|!\[(?P<alt>[^\]]*)\]\((?P<src>[^)\s]+)\)"
    r"|\[(?P<label>[^\]]+)\]\((?P<href>[^)\s]+)\)"
    r"|\*\*(?P<strong>.+?)\*\*"
    r"|(?<!\w)__(?P<strong_u>.+?)__(?!\w)"
    r"|(?<![\w*])\*(?P<em>[^*\s](?:[^*]*?[^*\s])?)\*(?![\w*])"
    r"|(?<!\w)_(?P<em_u>[^_\s](?:[^_]*?[^_\s])?)_(?!\w)"
)


def from_markdown(text: str) -> dict[str, Any]:
    """The ADF ``doc`` for markdown ``text``."""

    lines = _without_comments(text).split("\n")
    return {"type": "doc", "version": 1, "content": _parse_blocks(lines)}


def _without_comments(text: str) -> str:
    """``text`` with every HTML comment outside a code fence removed."""

    kept: list[str] = []
    fenced = False
    chunk: list[str] = []
    for line in text.split("\n"):
        if _FENCE.match(line):
            if not fenced:
                kept.append(_COMMENT.sub("", "\n".join(chunk)))
                chunk = []
                kept.append(line)
            else:
                kept.append("\n".join(chunk))
                chunk = []
                kept.append(line)
            fenced = not fenced
            continue
        chunk.append(line)
    kept.append("\n".join(chunk) if fenced else _COMMENT.sub("", "\n".join(chunk)))
    return "\n".join(kept)


def _starts_block(line: str) -> bool:
    return bool(
        _FENCE.match(line)
        or _HEADING.match(line)
        or _RULE.match(line)
        or _BULLET.match(line)
        or _ORDERED.match(line)
        or _QUOTE.match(line)
    )


def _parse_blocks(lines: list[str]) -> list[dict[str, Any]]:
    blocks: list[dict[str, Any]] = []
    i = 0
    while i < len(lines):
        line = lines[i]
        if not line.strip():
            i += 1
            continue
        fence = _FENCE.match(line)
        if fence:
            i = _code_block(lines, i, fence, blocks)
            continue
        heading = _HEADING.match(line)
        if heading:
            content = _parse_inline(heading.group(2))
            node: dict[str, Any] = {"type": "heading", "attrs": {"level": len(heading.group(1))}}
            if content:
                node["content"] = content
            blocks.append(node)
            i += 1
            continue
        if _RULE.match(line):
            blocks.append({"type": "rule"})
            i += 1
            continue
        if _BULLET.match(line) or _ORDERED.match(line):
            node, i = _parse_list(lines, i)
            blocks.append(node)
            continue
        if _QUOTE.match(line):
            quoted: list[str] = []
            while i < len(lines) and (match := _QUOTE.match(lines[i])):
                quoted.append(match.group(1))
                i += 1
            inner: list[dict[str, Any]] = []
            for child in _parse_blocks(quoted):
                if child["type"] == "heading":
                    child = {"type": "paragraph", "content": child.get("content", [])}
                elif child["type"] == "rule":
                    child = {
                        "type": "paragraph",
                        "content": [{"type": "text", "text": "---"}],
                    }
                if child["type"] == "blockquote":
                    inner.extend(child["content"])
                else:
                    inner.append(child)
            if inner:
                blocks.append({"type": "blockquote", "content": inner})
            continue
        paragraph: list[str] = []
        while (
            i < len(lines) and lines[i].strip() and (not paragraph or not _starts_block(lines[i]))
        ):
            paragraph.append(lines[i].strip())
            i += 1
        prose = _paragraph(paragraph)
        if prose is not None:
            blocks.append(prose)
    return blocks


def _code_block(
    lines: list[str], start: int, fence: re.Match[str], blocks: list[dict[str, Any]]
) -> int:
    marker = fence.group(1)
    language = fence.group(2)
    body: list[str] = []
    i = start + 1
    while i < len(lines) and not lines[i].strip().startswith(marker):
        body.append(lines[i])
        i += 1
    node: dict[str, Any] = {"type": "codeBlock"}
    if language:
        node["attrs"] = {"language": language}
    code = "\n".join(body)
    if code:
        node["content"] = [{"type": "text", "text": code}]
    blocks.append(node)
    return i + 1


def _paragraph(lines: Sequence[str]) -> dict[str, Any] | None:
    content: list[dict[str, Any]] = []
    for index, line in enumerate(lines):
        if index:
            content.append({"type": "hardBreak"})
        content.extend(_parse_inline(line))
    if not any(node["type"] != "hardBreak" for node in content):
        return None
    return {"type": "paragraph", "content": content}


def _list_match(line: str) -> tuple[int, bool, str] | None:
    """(indent, ordered, text) of a list item line, or None."""

    bullet = _BULLET.match(line)
    if bullet and not _RULE.match(line):
        return len(bullet.group(1).expandtabs(4)), False, bullet.group(2)
    ordered = _ORDERED.match(line)
    if ordered:
        return len(ordered.group(1).expandtabs(4)), True, ordered.group(3)
    return None


def _parse_list(lines: list[str], start: int) -> tuple[dict[str, Any], int]:
    first = _list_match(lines[start])
    assert first is not None
    indent, ordered, _ = first
    items: list[dict[str, Any]] = []
    i = start
    while i < len(lines):
        match = _list_match(lines[i])
        if match is None or match[0] != indent or match[1] != ordered:
            break
        text = [match[2].strip()]
        i += 1
        nested: list[dict[str, Any]] = []
        while i < len(lines) and lines[i].strip():
            inner = _list_match(lines[i])
            if inner is not None and inner[0] > indent:
                sublist, i = _parse_list(lines, i)
                nested.append(sublist)
                continue
            if inner is not None or _starts_block(lines[i]):
                break
            text.append(lines[i].strip())
            i += 1
        content: list[dict[str, Any]] = []
        paragraph = _paragraph(text)
        if paragraph is not None:
            content.append(paragraph)
        content.extend(nested)
        if not content:
            content.append({"type": "paragraph", "content": []})
        items.append({"type": "listItem", "content": content})
    node: dict[str, Any] = {"type": "orderedList" if ordered else "bulletList", "content": items}
    if ordered:
        node["attrs"] = {"order": 1}
    return node, i


def _parse_inline(text: str, marks: tuple[dict[str, Any], ...] = ()) -> list[dict[str, Any]]:
    nodes: list[dict[str, Any]] = []
    position = 0
    for match in _SPAN.finditer(text):
        if match.start() > position:
            nodes.append(_text(text[position : match.start()], marks))
        position = match.end()
        if match.group("escaped") is not None:
            nodes.append(_text(match.group("escaped"), marks))
        elif match.group("code") is not None:
            nodes.append(_text(match.group("code"), (*marks, {"type": "code"})))
        elif match.group("src") is not None:
            # An image becomes a link to it (decision 15): ADF media needs an upload.
            label = match.group("alt") or match.group("src")
            nodes.append(_text(label, (*marks, _link(match.group("src")))))
        elif match.group("href") is not None:
            nodes.extend(_parse_inline(match.group("label"), (*marks, _link(match.group("href")))))
        elif (strong := match.group("strong") or match.group("strong_u")) is not None:
            nodes.extend(_parse_inline(strong, (*marks, {"type": "strong"})))
        else:
            em = match.group("em") or match.group("em_u")
            nodes.extend(_parse_inline(em, (*marks, {"type": "em"})))
    if position < len(text):
        nodes.append(_text(text[position:], marks))
    return _merged(nodes)


def _decode_commonmark_entities(text: str) -> str:
    def replace(match: re.Match[str]) -> str:
        reference = match.group()
        if reference.startswith("&#"):
            return unescape(reference)
        return html5.get(reference[1:], reference)

    return _ENTITY_REFERENCE.sub(replace, text)


def _link(href: str) -> dict[str, Any]:
    return {"type": "link", "attrs": {"href": _decode_commonmark_entities(href)}}


def _text(text: str, marks: tuple[dict[str, Any], ...]) -> dict[str, Any]:
    if not any(mark["type"] == "code" for mark in marks):
        text = _decode_commonmark_entities(text)
    node: dict[str, Any] = {"type": "text", "text": text}
    if marks:
        node["marks"] = list(marks)
    return node


def _merged(nodes: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Adjacent text nodes with the same marks as one node; empty ones dropped."""

    merged: list[dict[str, Any]] = []
    for node in nodes:
        if node["type"] == "text" and not node["text"]:
            continue
        previous = merged[-1] if merged else None
        if (
            previous is not None
            and previous["type"] == node["type"] == "text"
            and previous.get("marks") == node.get("marks")
        ):
            previous["text"] += node["text"]
            continue
        merged.append(node)
    return merged
