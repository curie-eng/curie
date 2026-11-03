"""The UI evidence comment: its marker, its text and how much of it fits (plan 5.5).

The core owns the marker (`ui_evidence_marker`); each code host embeds it its
own way through `MarkedComments.upsert_marked`. Image URLs come only from the
code host's stored evidence, keyed by ordinal, so no manifest path or label
ever becomes a URL. Labels and paths appear only as escaped text.
"""

from __future__ import annotations

import re
import unicodedata
import uuid
from collections.abc import Sequence
from dataclasses import dataclass

# GitHub's comment body limit, in characters.
MAX_COMMENT_CHARS = 65536
# Room left for the embedded marker and any host framing around the body.
_FRAMING_RESERVE = 256
_LABEL_CHARS = 120
_PATH_CHARS = 200
_REASON_CHARS = 500

# One fixed sentence per refusal code (frozen in tests/vectors/ui-evidence-cases.json).
REFUSAL_TEXT: dict[str, str] = {
    "traversal": "its path leaves the evidence directory.",
    "invalid_entry": "its manifest entry is incomplete or malformed.",
    "symlink": "it is a symbolic link or sits under one.",
    "missing": "the file was not found.",
    "not_regular": "it is not a regular file.",
    "not_png": "it is not a PNG image.",
    "corrupt_png": "the PNG file is damaged.",
    "too_large": "it is larger than 10 MiB.",
    "total_limit": "the run's evidence passed 100 MiB in total.",
    "unbound": "captured from a different base or candidate than this pull request.",
    "transfer_failed": "it could not be copied out of the run.",
    "timeout": "it ran out of time before it could be published.",
    "not_stored": "it was not stored before publishing.",
    "head_changed": "captured from a different candidate than this pull request head.",
    "not_shown_size": "it did not fit in this comment.",
}
_UNKNOWN_REFUSAL = "it could not be shown."
# Why a revision's evidence was not published, for the stale notice.
_FAILURE_TEXT: dict[str, str] = {
    "timeout": "the publish window ran out",
    "upload_incomplete": "the upload from the run did not finish",
}
_DEFAULT_FAILURE = "the code host could not store it"
_FOOTER = "Screenshots were captured by the run and are not part of the code change."
_MARKDOWN_SPECIAL = re.compile(r"([\\`*_{}\[\]()#+\-.!|~])")
_CONTROL = re.compile(r"[\x00-\x1f\x7f-\x9f]")


def ui_evidence_marker(lineage_id: uuid.UUID) -> str:
    """The marker of the one evidence comment a lineage's pull request carries."""

    return f"curie-ui-evidence:{lineage_id}"


def body_limit(marker: str) -> int:
    """The characters a body may use, leaving room for the embedded marker."""

    return MAX_COMMENT_CHARS - len(f"\n\n<!-- {marker} -->\n") - _FRAMING_RESERVE


def _plain(value: str | None, limit: int) -> str:
    text = unicodedata.normalize("NFC", value or "")
    text = " ".join(_CONTROL.sub(" ", text).split())
    return text[:limit]


def escape_label(value: str | None, limit: int = _LABEL_CHARS) -> str:
    text = _plain(value, limit)
    text = text.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")
    return _MARKDOWN_SPECIAL.sub(r"\\\1", text)


def _alt(value: str | None) -> str:
    # Alt text closes at the first ']', so brackets never appear in it.
    text = _plain(value, _LABEL_CHARS).replace("[", "(").replace("]", ")")
    return text.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")


def code_path(value: str | None) -> str:
    return "`" + (_plain(value, _PATH_CHARS).replace("`", "'") or "(no path)") + "`"


def refusal_text(code: str) -> str:
    return REFUSAL_TEXT.get(code, _UNKNOWN_REFUSAL)


@dataclass(frozen=True)
class Entry:
    """One manifest entry as the comment sees it."""

    ordinal: int
    state: str | None
    label: str | None
    path: str | None


@dataclass(frozen=True)
class Shown:
    entry: Entry
    url: str


@dataclass(frozen=True)
class NotShown:
    entry: Entry
    code: str


@dataclass(frozen=True)
class Rendered:
    body: str
    outcome: str
    shown: tuple[int, ...]


def outcome_of(manifest_state: str, manifest_status: str | None, *, listed: int, shown: int) -> str:
    """Plan 5.5 rule 3: failed for an invalid manifest; otherwise the manifest
    status, complete downgraded to partial when an image is not shown, and
    unavailable when images were listed and none is shown."""

    if manifest_state != "valid":
        return "failed"
    if listed and not shown:
        return "unavailable"
    status = manifest_status or "failed"
    if status == "complete" and shown < listed:
        return "partial"
    return status


def _header(outcome: str, head_sha: str, revision: int, base_sha: str) -> list[str]:
    return [
        "### UI evidence",
        "",
        f"**Outcome:** {outcome}",
        f"**Pull request head:** `{head_sha}` (revision {revision})",
        f"**Base:** `{base_sha}`",
    ]


def _compose(
    *,
    outcome: str,
    head_sha: str,
    revision: int,
    base_sha: str,
    reason: str | None,
    shown: Sequence[Shown],
    not_shown: Sequence[NotShown],
    listed_limit: int,
) -> str:
    lines = _header(outcome, head_sha, revision, base_sha)
    if reason:
        lines.append(f"**Reason:** {escape_label(reason, _REASON_CHARS)}")
    for state, title in (("before", "Before"), ("after", "After")):
        blocks = [item for item in shown if item.entry.state == state]
        if not blocks:
            continue
        lines += ["", f"#### {title}"]
        for item in blocks:
            lines += [
                "",
                f"**{escape_label(item.entry.label)}** ({code_path(item.entry.path)})",
                "",
                f"![{_alt(item.entry.label)}]({item.url})",
            ]
    if not_shown:
        lines += ["", "#### Not shown", ""]
        listed = not_shown[:listed_limit]
        for number, missing in enumerate(listed, start=1):
            suffix = f" ({missing.entry.state})" if missing.entry.state else ""
            lines.append(
                f"{number}. {code_path(missing.entry.path)}{suffix}: {refusal_text(missing.code)}"
            )
        rest = len(not_shown) - len(listed)
        if rest:
            noun = "image is" if rest == 1 else "images are"
            lines.append(f"{len(listed) + 1}. {rest} more {noun} not shown.")
    lines += ["", _FOOTER]
    return "\n".join(lines) + "\n"


def render_evidence(
    *,
    marker: str,
    manifest_state: str,
    manifest_status: str | None,
    reason: str | None,
    head_sha: str,
    revision: int,
    base_sha: str,
    entries: Sequence[Entry],
    urls: dict[int, str],
    refusals: dict[int, str],
    outcome: str | None = None,
) -> Rendered:
    """The evidence comment: every entry with a URL is shown in manifest order
    (before, then after) while it fits; the rest are listed with their reason.

    ``refusals`` names why each entry without a URL is not shown. ``outcome``,
    when given, replaces the outcome the manifest and the shown images imply.
    """

    limit = body_limit(marker)
    candidates = [Shown(entry, urls[entry.ordinal]) for entry in entries if entry.ordinal in urls]
    ordered = [item for item in candidates if item.entry.state == "before"] + [
        item for item in candidates if item.entry.state != "before"
    ]
    refused = [
        NotShown(entry, refusals.get(entry.ordinal, "not_stored"))
        for entry in entries
        if entry.ordinal not in urls
    ]
    listed = len(entries)

    def build(count: int, listed_limit: int) -> str:
        shown = ordered[:count]
        overflow = [NotShown(item.entry, "not_shown_size") for item in ordered[count:]]
        not_shown = sorted(refused + overflow, key=lambda item: item.entry.ordinal)
        return _compose(
            outcome=outcome
            or outcome_of(manifest_state, manifest_status, listed=listed, shown=count),
            head_sha=head_sha,
            revision=revision,
            base_sha=base_sha,
            reason=reason,
            shown=shown,
            not_shown=not_shown,
            listed_limit=listed_limit,
        )

    total = len(refused) + len(ordered)
    for count in range(len(ordered), -1, -1):
        body = build(count, total)
        if len(body) <= limit:
            return Rendered(
                body,
                outcome or outcome_of(manifest_state, manifest_status, listed=listed, shown=count),
                tuple(item.entry.ordinal for item in ordered[:count]),
            )
    # Even without images the list is too long: list as many as fit, count the rest.
    low, high = 0, total
    while low < high:
        middle = (low + high + 1) // 2
        if len(build(0, middle)) <= limit:
            low = middle
        else:
            high = middle - 1
    return Rendered(
        build(0, low),
        outcome or outcome_of(manifest_state, manifest_status, listed=listed, shown=0),
        (),
    )


def render_unsupported(*, head_sha: str, captured: int) -> str:
    noun, verb = ("screenshot", "is") if captured == 1 else ("screenshots", "are")
    return (
        "### UI evidence\n\n"
        f"This code host cannot show UI evidence, so the {captured} {noun} captured for "
        f"head `{head_sha}` {verb} not shown.\n"
    )


def render_stale(*, head_sha: str, revision: int, failure: str | None) -> str:
    if failure is not None:
        why = _FAILURE_TEXT.get(failure, _DEFAULT_FAILURE)
        text = (
            f"UI evidence for the current pull request head `{head_sha}` (revision {revision}) "
            f"could not be published: {why}."
        )
    else:
        text = (
            f"No UI evidence exists for the current pull request head `{head_sha}` "
            f"(revision {revision}). Evidence from an earlier revision was removed."
        )
    return f"### UI evidence\n\n{text}\n"
