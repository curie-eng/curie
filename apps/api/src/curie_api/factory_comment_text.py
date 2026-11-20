"""Text every factory comment carries, whatever forge it is posted on.

The request marker that finds a status comment again, and the redaction a
comment body passes through before publication.
"""

from __future__ import annotations

import re
import unicodedata
import uuid
from urllib.parse import urlsplit

from curie_telemetry.redact import redact_text


def github_ask_line(mention: str) -> str:
    """How to ask the factory, for a GitHub tracker. Other forges do not use it."""

    return (
        "Ask the factory: comment on this pull request. "
        f"The first 10 revisions need no mention; after that, mention @{mention}."
    )


_ASK_PREFIX = "Ask the factory:"


def append_github_ask_line(body: str, mention: str) -> str:
    """Put that line last, dropping every earlier ask line, whatever its wording."""

    line = github_ask_line(mention)
    kept: list[str] = []
    for part in body.splitlines():
        if part.strip().startswith(_ASK_PREFIX):
            # Take the blank line that separated it too, so no gap is left behind.
            if kept and not kept[-1].strip():
                kept.pop()
            continue
        kept.append(part)
    text = "\n".join(kept).rstrip()
    text = f"{text}\n\n{line}" if text else line
    if body.endswith("\n"):
        text += "\n"
    return text


_FACTORY_URL = re.compile(r"https?://[^\s<>\"'`]+", re.IGNORECASE)
_KEY_MANAGEMENT_PATH = re.compile(
    r"/(?:api[-_]?keys|keys|key[-_]management)(?:[/?#]|$)", re.IGNORECASE
)
_PULL_REQUEST_PATH = re.compile(r"/pull/[1-9][0-9]*(?:[/?#]|$)")
# Horizontal space only: a label must not cross into the next publisher line
# and swallow ``Cause:``. Quotes accept JSON escapes so an inner \" does not
# end the value early. Emphasis or backticks may wrap the label, as in
# ``**key_id**:`` or `` `workspace`: ``.
_EMPHASIS = r"(?:[*_`]{1,3})?"
_QUOTED_PROVIDER_VALUE = r"(?:\"(?:\\.|[^\"\\])*\"|'(?:\\.|[^'\\])*'|`[^`\n]*`)"
_PROVIDER_TOKEN = r"[^\s,;)}>" + r"\"'`]+"
_PROVIDER_VALUE = r"(?P<value>" + _QUOTED_PROVIDER_VALUE + r"|" + _PROVIDER_TOKEN + r")"
_PROVIDER_LABEL_TAIL = r"[\"']?[ \t]*(?=[:=\"'`])(?:[:=][ \t]*)?"
# ``_`` is a word character, so ``\b`` misses ``_key_id_``. Bound the label
# on letters and digits instead, then allow emphasis on either side.
_LABEL_BEFORE = r"(?<![A-Za-z0-9])"
_LABEL_AFTER = r"(?![A-Za-z0-9])"
_PROVIDER_KEY_ID = re.compile(
    r"(?P<label>"
    + _EMPHASIS
    + _LABEL_BEFORE
    + r"(?:api[ _-]?)?key[ _-]?(?:id|identifier|hash)"
    + _LABEL_AFTER
    + _EMPHASIS
    + _PROVIDER_LABEL_TAIL
    + r")"
    + _PROVIDER_VALUE,
    re.IGNORECASE,
)
# The name phrase stays case sensitive so lowercase diagnostic prose
# (``failed to prepare the repository``) is not a workspace name. A capitalized
# phrase (``Acme Research Team``) is. A slug still matches in any case.
_SLUG_TOKEN = r"(?![a-z]+[ \t]+[a-z])(?=[^\s,;)}>" + r"\"'`]*[0-9_-])" + _PROVIDER_TOKEN
_WORKSPACE_NAME = r"[A-Z][A-Za-z0-9._-]*(?:[ \t]+[A-Z][A-Za-z0-9._-]*)*" r"|" + _SLUG_TOKEN
_PROVIDER_WORKSPACE = re.compile(
    r"(?P<label>"
    + _EMPHASIS
    + _LABEL_BEFORE
    + r"(?i:workspace(?:[ _-]?name)?)"
    + _LABEL_AFTER
    + _EMPHASIS
    + _PROVIDER_LABEL_TAIL
    + r")(?P<value>"
    + _QUOTED_PROVIDER_VALUE
    + r"|"
    + _WORKSPACE_NAME
    + r")"
)
# ``workspace=acme`` is an assignment, not diagnostic prose, so the value may
# be a plain word. The colon form stays on ``_PROVIDER_WORKSPACE``.
_PROVIDER_WORKSPACE_ASSIGN = re.compile(
    r"(?P<label>"
    + _EMPHASIS
    + _LABEL_BEFORE
    + r"(?i:workspace(?:[ _-]?name)?)"
    + _LABEL_AFTER
    + _EMPHASIS
    + r"[ \t]*=[ \t]*)(?P<value>"
    + _QUOTED_PROVIDER_VALUE
    + r"|"
    + _PROVIDER_TOKEN
    + r")"
)


def redact_factory_comment(body: str) -> str:
    """Redact secrets and contextual provider identifiers before publication."""

    def redact_url(match: re.Match[str]) -> str:
        url = match[0]
        trimmed = url.rstrip(".,;:!)")
        try:
            parsed = urlsplit(trimmed)
            path = parsed.path
            github_pr = parsed.hostname in {"github.com", "www.github.com"} and bool(
                _PULL_REQUEST_PATH.search(path)
            )
        except ValueError:
            path = trimmed
            github_pr = False
        if _KEY_MANAGEMENT_PATH.search(path) and not github_pr:
            return "[REDACTED:provider_key_url]" + url[len(trimmed) :]
        return url

    body = _FACTORY_URL.sub(redact_url, body)
    body = redact_text(body)
    for pattern, label in (
        (_PROVIDER_KEY_ID, "provider_key_id"),
        (_PROVIDER_WORKSPACE_ASSIGN, "provider_workspace"),
        (_PROVIDER_WORKSPACE, "provider_workspace"),
    ):

        def redact_identifier(match: re.Match[str], label: str = label) -> str:
            placeholder = f"[REDACTED:{label}]"
            value = match["value"]
            if value == placeholder or value in {
                f'"{placeholder}"',
                f"'{placeholder}'",
                f"`{placeholder}`",
            }:
                return match[0]
            if re.search(r"[A-Za-z0-9]", value) is None:
                return match[0]
            return f"{match['label']}{placeholder}"

        body = pattern.sub(redact_identifier, body)
    return body


def status_marker(request_id: uuid.UUID) -> str:
    """The core-owned marker of one request's status comment; a tracker
    adapter decides how to carry it (`MarkedComments.upsert_marked`)."""

    return f"curie-execution-request:{request_id}"


def marker_for(request_id: uuid.UUID) -> str:
    return f"<!-- {status_marker(request_id)} -->"


def without_controls(text: str) -> str:
    """The text with no control or invisible format character.

    Drops C0 (newline and tab kept), DEL, C1, every format character
    (category ``Cf``: the bidi embeddings, overrides and isolates, the
    marks U+200E/U+200F/U+061C, zero-width joiners and spaces, the BOM, tag
    characters) and lone surrogates. A line or paragraph separator becomes a
    newline. A right-to-left override would otherwise reorder the App's own
    lines a reader sees below the model's text.
    """

    kept: list[str] = []
    for char in text.replace("\r\n", "\n"):
        category = unicodedata.category(char)
        if char in "\n\t":
            kept.append(char)
        elif category in ("Zl", "Zp"):
            kept.append("\n")
        elif category not in ("Cc", "Cf", "Cs"):
            kept.append(char)
    return "".join(kept)


def publishable_model_text(text: str) -> str:
    """Redacted, with no control character, no HTML comment opener left to
    carry a marker, and no live mention.

    Controls go first, so a zero-width character cannot split a secret past
    redaction. The tracker posts this text under the App's identity, so a
    model-written ``@login`` would notify that account. A U+200B after every
    ``@`` (added after the strip) keeps the text readable and stops the
    mention, as `factory_notices` does for the agent's last message. The tracker
    result and the question comment (#5785) both post model text this way.
    """

    return (
        redact_factory_comment(without_controls(text))
        .replace("<!--", "&lt;!--")
        .replace("@", "@\u200b")
    )
