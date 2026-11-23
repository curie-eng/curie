"""The declared no-change marker accepted for an open factory follow-up."""

from __future__ import annotations


def no_change_marker_line(detail: str | None) -> str | None:
    """Return the first eligible marker line, ignoring leading quote/emphasis marks."""

    if not detail:
        return None
    for line in detail.splitlines():
        candidate = line.lstrip().lstrip(">*_").lstrip()
        if candidate.startswith("No changes needed:"):
            return candidate
    return None
